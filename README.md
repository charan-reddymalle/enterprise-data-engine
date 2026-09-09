# Enterprise Data Engine

A production-grade, metadata-driven data platform framework built on **PySpark**, **Delta Lake**, and **Databricks Asset Bundles (DABs)**. Engineered to replace brittle, notebook-based workflows with modular software engineering practices: declarative configuration management, automated data quality quarantine routing, native Unity Catalog security policies, and zero-downtime CI/CD deployment gates.

---

## Key Capabilities

* **Metadata-Driven Ingestion & Modeling:** Decouples orchestration logic from dataset schemas. Tables, joins, derivations, and projections are declared via YAML blueprints.
* **Unified Scope-Aware Delta Merge Engine:** Standardizes `RELOAD`, `UPSERT`, and `APPEND` writes with partition-level scoping, schema conformance, and idempotency guarantees.
* **Active Quality Quarantine Gate:** Eliminates passive error logging by inspecting constraints against `system.information_schema` in a single pass. Automatically isolates invalid records into dedicated quarantine tables while routing clean data to target layers.
* **Automated Data Protection & Dynamic Masking:** Programmatically queries classification tags from Microsoft Purview, reconciles active masks across catalogs, and applies column-level encryption (`AES-256`) and dynamic role-based masks (`ALTER TABLE SET MASK`).
* **Automated Schema Lifecycle Waves:** Parses relational schema blueprints to orchestrate parallel DDL provisioning across priority waves (`Dimensions` $\rightarrow$ `Facts` $\rightarrow$ `Views`).
* **Shift-Left CI/CD Automation:** Multi-stage Azure Pipelines and local testing suites that enforce linting (`ruff`), configuration formatting (`pyproject-fmt`), unit testing (`pytest`), and bundle validation before production deployment.

---

## System Architecture
```mermaid
flowchart TD
    subgraph ConfigPlane [Declarative Configuration Plane]
        CFG[YAML Blueprints & Schemas]
        PURVIEW[Microsoft Purview DataMap API]
    end

    subgraph IngestionPlane [Raw Ingestion Tier]
        BRONZE[(Bronze Landing Tables)]
    end

    subgraph ProcessingCore [Enterprise Data Engine]
        direction TB
        PROJ[Dynamic Ingestion & Blueprint Reader]
        STND[KPI & Currency Standardizer]
        DQE{Active DQ Validation Gate}
        MERGE[Scoped Delta Merge Engine]
        PROT[Dynamic Masking & UC Policy Manager]
    end

    subgraph StoragePlane [Delta Lakehouse]
        SILVER[(Silver Conformed Tables)]
        QUARANTINE[(Quarantine Audit Vault)]
        GOLD[(Gold Consumption Views)]
    end

    CFG --> PROJ
    PURVIEW --> PROT
    BRONZE --> PROJ
    PROJ --> STND
    STND --> DQE

    DQE -->|Valid Records| MERGE
    DQE -->|Invalid Records| QUARANTINE
    MERGE --> SILVER
    SILVER --> PROT
    PROT --> GOLD
```