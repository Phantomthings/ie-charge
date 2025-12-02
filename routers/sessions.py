"""
Router pour les sessions de charge
Endpoint: GET /api/sessions/stats
"""

from fastapi import APIRouter, Request, Query
from fastapi.templating import Jinja2Templates
from datetime import date
import pandas as pd
import numpy as np

from db import query_df

router = APIRouter(tags=["sessions"])
templates = Jinja2Templates(directory="templates")


@router.get("/sessions/stats")
async def get_sessions_stats(
    request: Request,
    sites: str = Query(default=""),
    date_debut: date = Query(default=None),
    date_fin: date = Query(default=None),
    error_types: str = Query(default=""),
    moments: str = Query(default=""),
):
    """
    Retourne les statistiques globales des sessions (taux réussite, échecs)
    """
    error_type_list = [e.strip() for e in error_types.split(",") if e.strip()] if error_types else []
    moment_list = [m.strip() for m in moments.split(",") if m.strip()] if moments else []

    # Construire la requête avec filtres
    conditions = ["1=1"]
    params = {}
    
    if date_debut:
        conditions.append("`Datetime start` >= :date_debut")
        params["date_debut"] = str(date_debut)
    if date_fin:
        conditions.append("`Datetime start` < DATE_ADD(:date_fin, INTERVAL 1 DAY)")
        params["date_fin"] = str(date_fin)
    if sites:
        site_list = [s.strip() for s in sites.split(",") if s.strip()]
        if site_list:
            placeholders = ",".join([f":site_{i}" for i in range(len(site_list))])
            conditions.append(f"Site IN ({placeholders})")
            for i, s in enumerate(site_list):
                params[f"site_{i}"] = s
    
    where_clause = " AND ".join(conditions)
    
    sql = f"""
        SELECT
            Site,
            `State of charge(0:good, 1:error)` as state,
            type_erreur,
            moment
        FROM kpi_sessions
        WHERE {where_clause}
    """

    df = query_df(sql, params)
    
    if df.empty:
        return templates.TemplateResponse(
            "partials/sessions_stats.html",
            {
                "request": request,
                "total": 0,
                "ok": 0,
                "nok": 0,
                "taux_reussite": 0,
                "taux_echec": 0,
                "stats_par_site": [],
            }
        )

    df["is_ok"] = pd.to_numeric(df["state"], errors="coerce").fillna(0).astype(int).eq(0)

    # Appliquer les filtres d'erreur au statut OK/NOK
    mask_nok = ~df["is_ok"]

    if error_type_list and "type_erreur" in df.columns:
        mask_type = df["type_erreur"].isin(error_type_list)
    else:
        mask_type = True

    if moment_list and "moment" in df.columns:
        mask_moment = df["moment"].isin(moment_list)
    else:
        mask_moment = True

    mask_nok_keep = mask_nok & mask_type & mask_moment
    df["is_ok_filt"] = np.where(mask_nok_keep, False, True)

    total = len(df)
    ok = int(df["is_ok_filt"].sum())
    nok = total - ok
    taux_reussite = round(ok / total * 100, 1) if total else 0
    taux_echec = round(nok / total * 100, 1) if total else 0

    # Stats par site
    stats_site = (
        df.groupby("Site")
        .agg(
            total=("is_ok_filt", "count"),
            ok=("is_ok_filt", "sum"),
        )
        .reset_index()
    )
    stats_site["nok"] = stats_site["total"] - stats_site["ok"]
    stats_site["taux_ok"] = np.where(
        stats_site["total"] > 0,
        (stats_site["ok"] / stats_site["total"] * 100).round(1),
        0
    )
    
    # Top 10 par volume
    top_sites = stats_site.sort_values("total", ascending=False).head(10)
    
    # Top 10 par échecs
    top_echecs = stats_site.sort_values("nok", ascending=False).head(10)
    
    return templates.TemplateResponse(
        "partials/sessions_stats.html",
        {
            "request": request,
            "total": total,
            "ok": ok,
            "nok": nok,
            "taux_reussite": taux_reussite,
            "taux_echec": taux_echec,
            "top_sites": top_sites.to_dict("records"),
            "top_echecs": top_echecs.to_dict("records"),
        }
    )
