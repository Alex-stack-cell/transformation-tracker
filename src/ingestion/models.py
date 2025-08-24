from pydantic import BaseModel
from datetime import datetime

# Models de données


class InitiativeData(BaseModel):
    initiative_id: str
    name: str
    type: str  # Digital, Operational, HR, Financial
    start_date: datetime
    target_end_date: datetime
    budget_allocated: float
    budget_spent: float
    status: str  # Planning, In Progress, At Risk, Completed
    owner: str
    description: str


class FinancialMetric(BaseModel):
    initiative_id: str
    date: datetime
    revenue_impact: float
    cost_reduction: float
    roi_percentage: float
    budget_burn_rate: float
    forecast_completion_cost: float


class OperationalMetric(BaseModel):
    initiative_id: str
    date: datetime
    efficiency_gain_percentage: float
    process_cycle_time: float  # in hours
    quality_score: float  # 0-100
    employee_satisfaction: float  # 0-10
    customer_satisfaction: float  # 0-10
