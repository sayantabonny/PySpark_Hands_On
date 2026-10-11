"""Reference data shared by the generators and the expectation logic (single source of truth)."""
from __future__ import annotations

PLACEHOLDER_CUSTOMER = "C0000000"

PRODUCTS: tuple[str, ...] = (
    "LOGIN-SSO", "BILLING-PORTAL", "API-GATEWAY", "WEB-CONSOLE", "MOBILE-IOS", "MOBILE-ANDROID",
    "REPORTING", "DATA-EXPORT", "INTEGRATIONS-HUB", "PAYMENTS", "NOTIFICATIONS", "USER-ADMIN",
    "AUDIT-LOG", "SEARCH", "DASHBOARDS", "WORKFLOW-ENGINE", "FILE-STORAGE", "CALENDAR-SYNC",
    "CRM-CONNECTOR", "ERP-CONNECTOR", "WEBHOOKS", "CLI-TOOLS", "SDK-PYTHON", "SDK-JAVA", "SCHEDULER",
    "ALERTING", "BACKUP", "IDENTITY-MFA", "LICENSING", "MARKETPLACE", "CHAT-WIDGET", "KNOWLEDGE-BASE",
    "EMAIL-DELIVERY", "SMS-GATEWAY", "ANALYTICS", "DATA-IMPORT", "SANDBOX", "STATUS-PAGE",
    "TRANSLATION", "ONBOARDING")
HOT_PRODUCT = "LOGIN-SSO"
UNKNOWN_PRODUCTS: tuple[str, ...] = ("AI-COPILOT", "EDGE-AGENT", "VOICE-BOT")   # new, not in DQ list

CATEGORIES: tuple[str, ...] = ("ACCOUNT_ACCESS", "BILLING", "BUG", "PERFORMANCE", "INTEGRATION",
                               "FEATURE_REQUEST", "DATA_REQUEST", "OTHER")

# product family -> the category it mostly generates (the rest is spread over all categories)
_AFFINITY_RULES: tuple[tuple[tuple[str, ...], str], ...] = (
    (("LOGIN", "IDENTITY", "USER-ADMIN"), "ACCOUNT_ACCESS"),
    (("BILLING", "PAYMENTS", "LICENSING", "MARKETPLACE"), "BILLING"),
    (("CONNECTOR", "INTEGRATIONS", "WEBHOOKS", "API", "SDK"), "INTEGRATION"),
    (("EXPORT", "IMPORT", "BACKUP", "AUDIT"), "DATA_REQUEST"),
    (("REPORTING", "DASHBOARDS", "ANALYTICS", "SEARCH"), "PERFORMANCE"),
)


def primary_category(product: str) -> str:
    for needles, category in _AFFINITY_RULES:
        if any(n in product for n in needles):
            return category
    return "BUG"


AGENT_CATEGORY = {"ACCOUNT_ACCESS": "login", "BILLING": "billing", "BUG": "bug", "PERFORMANCE": "slow",
                  "INTEGRATION": "integration", "FEATURE_REQUEST": "feature", "DATA_REQUEST": "data",
                  "OTHER": "other"}
QUEUE = {"ACCOUNT_ACCESS": "ACCESS", "BILLING": "BILLING", "BUG": "TECH", "PERFORMANCE": "TECH",
         "INTEGRATION": "TECH", "FEATURE_REQUEST": "GENERAL", "DATA_REQUEST": "GENERAL", "OTHER": "GENERAL"}
TAGS = {"ACCOUNT_ACCESS": ("login", "sso"), "BILLING": ("billing", "invoice"), "BUG": ("bug",),
        "PERFORMANCE": ("latency",), "INTEGRATION": ("api", "integration"), "FEATURE_REQUEST": ("idea",),
        "DATA_REQUEST": ("data", "gdpr"), "OTHER": ()}

# helpdesk source vocabulary (value maps in conf/sources/helpdesk_tickets.yml invert these)
HD_PRIORITY = {"P1": "urgent", "P2": "high", "P3": "normal", "P4": "low"}
HD_STATUS = {"NEW": "new", "OPEN": "open", "PENDING": "pending", "RESOLVED": "solved", "CLOSED": "closed"}
HD_CHANNELS: tuple[tuple[str, float], ...] = (("email", 45.0), ("chat", 25.0), ("web", 20.0),
                                               ("social", 5.0), ("api", 5.0))
PRIORITY_MIX: tuple[tuple[str, float], ...] = (("P1", 3.0), ("P2", 17.0), ("P3", 60.0), ("P4", 20.0))
PHONE_PRIORITY_MIX: tuple[tuple[str, float], ...] = (("P1", 5.0), ("P2", 25.0), ("P3", 55.0), ("P4", 15.0))

# call-centre vocabulary (the pipeline maps DISPOSITION to a canonical status, PDF 1 22.2)
DISPOSITIONS: tuple[tuple[str, float], ...] = (("RESOLVED", 70.0), ("ESCALATED", 15.0),
                                                ("CALLBACK", 8.0), ("ABANDONED", 7.0))

CALLCENTER_COLUMNS: tuple[str, ...] = ("CALL_ID", "CUST_NO", "CALL_START", "CALL_END", "AGENT", "QUEUE",
                                       "PRIORITY_CD", "DISPOSITION", "HANDLE_SECS", "NOTES",
                                       "PRODUCT_LINE", "CSAT", "IVR_JSON")
CRM_COLUMNS: tuple[str, ...] = ("customer_id", "customer_name", "tier", "region", "industry",
                                "signup_date", "is_active")

TIERS: tuple[tuple[str, float], ...] = (("ENTERPRISE", 5.0), ("BUSINESS", 25.0), ("STANDARD", 70.0))
REGIONS: tuple[tuple[str, float], ...] = (("APAC", 35.0), ("EMEA", 30.0), ("NA", 25.0), ("LATAM", 10.0))
INDUSTRIES: tuple[str, ...] = ("Retail", "Banking", "Insurance", "Healthcare", "Manufacturing",
                               "Logistics", "Education", "Media", "Telecom", "Public Sector")
LEGAL_SUFFIX = {"APAC": ("Pvt. Ltd.", "Pte. Ltd.", "K.K."), "EMEA": ("GmbH", "S.A.", "Ltd", "B.V."),
                "NA": ("Inc.", "LLC", "Corp."), "LATAM": ("S.A. de C.V.", "Ltda.")}
LOCALES = {"en": ("en-IN", "en-US", "en-GB", "en-AU", "en-SG")}      # English only (ASCII data)

HELPDESK_AGENTS = 300          # AGT-0001 .. AGT-0300
CALLCENTER_AGENTS = 100        # AGT-0301 .. AGT-0400


def customer_id(num: int) -> str:
    return f"C{num:07d}"


def agent_id(num: int) -> str:
    return f"AGT-{num:04d}"
