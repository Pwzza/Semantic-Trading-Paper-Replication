# Semantic Trading: Polymarket Replication & Post-Mortem

This repository contains an end-to-end Python replication of the academic paper **"Semantic Trading: Agentic AI for Clustering and Relationship Discovery in Prediction Markets"** (Capponi et al.). 


## 🛠️ System Pipeline

```mermaid
graph TD
    subgraph Phase 1: Data Infrastructure
        A[Gamma API] -->|Upstream Fetcher| B[(Raw Metadata Parquet)]
        B -->|DuckDB Filter| C[target_markets.csv VIP List]
        C -->|Queue Hijack| D[Upstream Workers]
        D -->|Rate Limited| E[CLOB / Data APIs]
        E --> F[(Prices & Trades Parquet)]
    end

    subgraph Phase 2: Semantic Clustering
        C --> G[Sentence-Transformers Embeddings]
        G --> H[K-Means Clustering K=77]
        H --> I[Gemma 3: Cluster Labeling]
        I --> J[clustered_markets.csv]
    end

    subgraph Phase 3: Agentic Trading
        J --> K[Gemma 3: Relationship Discovery]
        K -->|Batch Prompting| L[discovered_relationships.csv]
        L --> M{Backtest Engine}
        F -->|DuckDB Parquet Indexing| M
        M -->|Leader-Follower Logic| N[Trade Log & ROI Metrics]
    end
