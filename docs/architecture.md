
mckinsey-transformation-tracker/
├── src/
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── mock_apis.py          # Simulation des sources de données
│   │   ├── data_collectors.py    # Collecte des données
│   │   └── validators.py         # Validation qualité
│   ├── transformation/
│   │   ├── __init__.py
│   │   ├── cleaners.py           # Nettoyage des données
│   │   ├── calculators.py        # Calculs business metrics
│   │   └── aggregators.py        # Agrégations et marts
│   ├── analytics/
│   │   ├── __init__.py
│   │   ├── metrics.py            # KPIs et métriques
│   │   ├── models.py             # Modèles prédictifs simples
│   │   └── reports.py            # Génération de rapports
│   ├── monitoring/
│   │   ├── __init__.py
│   │   ├── quality_checks.py     # Tests qualité données
│   │   ├── performance.py        # Monitoring performance
│   │   └── alerts.py             # Système d'alertes
│   └── dashboard/
│       ├── __init__.py
│       ├── app.py                # Streamlit dashboard
│       └── components/           # Composants dashboard
├── config/
│   ├── __init__.py
│   ├── settings.py               # Configuration
│   └── database.py               # Config base de données
├── data/
│   ├── raw/                      # Données brutes
│   ├── processed/                # Données transformées
│   └── analytics/                # Données prêtes pour analytics
├── docs/
│   ├── architecture.md           # Documentation architecture
│   ├── api_specs.md             # Spécifications APIs
│   └── business_requirements.md  # Requirements business
├── tests/
│   ├── test_ingestion.py
│   ├── test_transformation.py
│   └── test_analytics.py
├── requirements.txt
├── README.md
├── .env                          # Variables d'environnement
└── .gitignore