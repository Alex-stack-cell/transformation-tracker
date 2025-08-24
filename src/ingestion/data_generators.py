from models import InitiativeData
import uuid
from datetime import datetime, timedelta
from faker import Faker
import random
from typing import List
fake = Faker()

# Données simulées
INITIATIVE_TYPES = ["Digital", "Operational", "HR", "Financial"]
STATUSES = ["Planning", "In Progress", "At Risk", "Completed", "On Hold"]


def generate_initiatives(count: int = 12) -> List[InitiativeData]:
    """Génère des initiatives de transformation réalistes"""
    initiatives: List[InitiativeData] = []

    initiative_names = [
        "CRM Migration to Salesforce",
        "Supply Chain Automation",
        "Remote Work Infrastructure",
        "Cost Reduction Program",
        "Digital Customer Portal",
        "Lean Manufacturing Implementation",
        "Skills Training Program",
        "ERP System Upgrade",
        "Process Digitization",
        "Vendor Consolidation",
        "Quality Management System",
        "Data Analytics Platform"
    ]

    for i in range(count):
        start_date = fake.date_between(start_date='-6m', end_date='-1m')
        start_datetime = datetime.combine(start_date, datetime.min.time())
        initiative = InitiativeData(
            initiative_id=str(uuid.uuid4()),
            name=initiative_names[i % len(initiative_names)],
            type=random.choice(INITIATIVE_TYPES),
            start_date=start_datetime,
            target_end_date=start_datetime +
            timedelta(days=random.randint(90, 365)),
            budget_allocated=random.uniform(100000, 2000000),
            budget_spent=random.uniform(50000, 1800000),
            status=random.choice(STATUSES),
            owner=fake.name(),
            description=fake.text(max_nb_chars=200)
        )
        initiatives.append(initiative)

    return initiatives
