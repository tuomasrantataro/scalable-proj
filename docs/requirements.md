# Requirements: Detecting Trading Trends in Financial Tick Data

Course: CS-E4780 Scalable Systems and Data Management, course project 2026
Responsible teacher: Prof. Bo Zhao (cs-e4780@aalto.fi)
**Submission deadline: 27.11.2026 (the last lecture)**

Sources: `CS_E4780_Course_Project_2026.pdf`, `Evaluation_Criteria_26.pdf` (both in this folder).

This document lists *what* the system must do. It deliberately does not choose technologies.

---

## 1. Overview

Build a scalable system that processes a week of real financial tick data and:

1. **Query 1:** computes exponential moving averages (EMA) of price per symbol over 5-minute tumbling windows.
2. **Query 2:** detects breakout patterns (EMA crossovers) and emits buy/sell advisories.
3. **Smart visualization (optional, bonus):** presents the query results to users in an interpretable way.

The system simulates a market data platform. Metrics are computed for every symbol, but results are delivered only to the symbols that traders subscribe to.

---

## 2. Input Data

### 2.1 Data set
- **Name:** DEBS 2022 Grand Challenge Data Set: Trading Data (Frischbier et al., 2022), https://doi.org/10.5281/zenodo.6382482
- **License:** CC BY-NC-SA 4.0
- **Contents:** about 289 million events (tick data plus housekeeping events)
- **Period:** Monday 8 Nov 2021 to Sunday 14 Nov 2021 (one week)
- **Instruments:** 5504 equities and indices
- **Exchanges:** Paris (`FR`), Amsterdam (`NL`), Frankfurt/Xetra (`ETR`)
- **Format:** flat CSV files, **one file per day**, one event per line

### 2.2 Data characteristics the system must cope with
| Property | Detail |
|---|---|
| Traffic by source | Paris 55%, Amsterdam 28%, Frankfurt 17% |
| Events by instrument type | Equities about 239.5 M, indices about 49.5 M |
| Actual trading (price) events | Indices about 49 M, equities about 10 M |
| Symbol distribution | **Long-tail:** most traffic comes from a small number of symbols |
| Ordering | **Total order of events per symbol across exchanges cannot be assumed** |
| Payload | Some events have no payload, because attributes were removed from the data set |
| Event type | **Not explicitly marked.** It is inferred from which attributes are non-NULL (e.g. `Ask` set means an ask event) |
| Rates | Reflects real per-second update rates, including peaks |

### 2.3 Attributes relevant to the project (★ in Table 1)
| # | Attribute | Meaning |
|---|---|---|
| 1 | `ID.[Exchange]` | Unique symbol identifier including exchange code (e.g. `RDSA.NL`) |
| 2 | `SecType` | Security type: `E` (equity) or `I` (index) |
| 22 | `Last` | Last trade price |
| 24 | `Trading time` | Time of last update (bid/ask/trade) |
| 27 | `Trading date` | Date of last trade |

- Timestamp format: `HH:MM:SS.ssss` (global **CEST**)
- Date format: `DD-MM-YYYY`
- Identifiers are already normalized by the data provider. Date and CEST timestamps have been added as enrichment.
- The full attribute list (39 attributes) is in Table 1 of the project description. The other attributes may be parsed or ignored.

---

## 3. Functional Requirements

### 3.1 General definitions and relaxations
- **FR-1** Each instrument instance is identified by a symbol `s ∈ S`: a unique string plus the exchange code (e.g. `RDSA.NL`).
- **FR-2** All events must be **grouped (partitioned) by symbol**.
- **FR-3** All metrics must be **computed continuously for all symbols** `s ∈ S`.
- **FR-4** **Updates (outputs) only need to be delivered for a subset of symbols** `S' ⊊ S` that the platform subscribes to (simulating traders). The subscription set must be configurable.
- **FR-5** Windows are **5 minutes** long.
- **FR-6** The first window `w0` starts at **00:00 (midnight) CEST**.
- **FR-7** Windows are **tumbling** (non-overlapping).
- **FR-8** Window `w_i` is **evaluated once the next window `w_{i+1}` starts**.
- **FR-9** The system must correctly ingest the raw CSV tick data, identify price events (non-NULL `Last`), and handle housekeeping and other event types without failing.

### 3.2 Query 1: Quantitative indicators (EMA)
- **FR-10** For each symbol `s` and window `w_i`, compute `Close(s, w_i)`: the **last price event** observed for `s` in `w_i`.
- **FR-11** Compute the EMA for both smoothing factors `j ∈ {38, 100}`:

  ```
  EMA^j(s, w_i) = Close(s, w_i) · (2 / (1 + j)) + EMA^j(s, w_{i-1}) · (1 − 2 / (1 + j))
  ```

- **FR-12** Initial condition: `EMA^j(s, w_0) = 0`. For the first reading of a symbol in the very first window, the previous window's EMA is taken as zero.
- **FR-13** EMA state must persist per symbol across windows (and across day boundaries, since the data spans a week).
- **FR-14** Output EMA38 and EMA100 per window for subscribed symbols.

### 3.3 Query 2: Breakout patterns (crossovers)
Uses `j1 = 38` (short) and `j2 = 100` (long).

- **FR-15 Bullish breakout, buy advisory.** Detected for symbol `s` at window `w_i` **if and only if**:
  ```
  EMA^38(s, w_i)     >  EMA^100(s, w_i)        (1)
  EMA^38(s, w_{i-1}) ≤  EMA^100(s, w_{i-1})    (2)
  ```
  A **buy advice event** must then be generated **immediately**.

- **FR-16 Bearish breakout, sell advisory.** Detected for symbol `s` at window `w_i` **if and only if**:
  ```
  EMA^38(s, w_i)     <  EMA^100(s, w_i)        (3)
  EMA^38(s, w_{i-1}) ≥  EMA^100(s, w_{i-1})    (4)
  ```
  A **sell advice event** must then be generated **immediately**.

- **FR-17** Advisories must be correct (no missed or spurious advisories) for **all subscribed symbols**.
- **FR-18** An advisory should identify at least the symbol, the type (buy/sell), and the window or timestamp it refers to. *(The exact output format is not specified and is ours to decide.)*

### 3.4 Smart visualization (optional, bonus)
- **FR-19** Provide a visualization of query results (EMAs, crossovers, advisories) that "cuts through the noise" and highlights the relevant data for the user.
- Counts toward the *Creative explorations* criterion (up to Exceptional).

---

## 4. Non-Functional Requirements

### 4.1 Performance
- **NFR-1** Process the **entire week** of data (about 289 M events) smoothly.
- **NFR-2** Sustain **high throughput** with **low and stable latency**, including during peak event rates.
- **NFR-3** Advisories must be emitted promptly ("immediately") after the triggering window is evaluated.

### 4.2 Scalability
- **NFR-4** Handle data from **all three exchanges** at full volume.
- **NFR-5** Cope with **peak rates** and the **long-tail symbol distribution** (hot-key / skew handling).
- **NFR-6** Leave room for growth: more symbols, more subscribers, more resources (horizontal and/or vertical scaling).

### 4.3 Correctness and robustness
- **NFR-7** Handle **out-of-order and late events** (no total order per symbol across exchanges). The chosen strategy must be justified.
- **NFR-8** Handle events with no payload or irrelevant event types gracefully.
- **NFR-9** Results must be verifiable. Correctness should be demonstrated, e.g. against a reference implementation.

### 4.4 Observability and measurability
- **NFR-10** The system must allow measuring throughput (events/s), end-to-end latency, and resource utilization (CPU, GPU if used, memory) under different loads and resource configurations.

### 4.5 Design exploration
- **NFR-11** Explore and justify design alternatives, preferably backed by measurements. Examples:
  - partitioning strategy (e.g. by symbol)
  - state management for windows and EMA
  - windowing approach
  - handling of late and out-of-order events

---

## 5. Deliverables

### 5.1 Code
- **D-1** A **public GitHub repository** containing the system.

### 5.2 Report
- **D-2** **4–5 pages**, **ACM Proceedings template** (LaTeX or Word).
- **D-3** Required sections (titles may differ, but the content must match):
  1. **Abstract**
  2. **Introduction:** background and overview
  3. **System Architecture:** design choices and motivations in detail
  4. **Implementation:** how it was built, including hardware or cloud services, programming tools, and GUI tools
  5. **Performance Evaluation:**
     - correctness (accuracy of query results)
     - scalability (behaviour with varying resources such as CPU, and workloads such as events/s)
     - resource utilization (CPU/GPU)
     - throughput and latency under load
     - handling of different event types and of the long-tail distribution
  6. **Conclusion**
  - References with a consistent citation format, and a strong focus on **dataset analysis**
- **D-4** For group projects, **clearly specify each member's contributions**.
- **D-5** Each member answers individually in the report:
  1. What are the most important decisions you and your group have made about the system design?
  2. What were the most challenging parts during design and implementation?
  3. What is your specific contribution to the team? How do you characterize the team's overall functionality?

---

## 6. Grading Summary (26 points)

| Area | Points | Criteria (each 0–3 unless noted) |
|---|---|---|
| System implementation | 12 | Functionality, Performance, Scalability, Creative explorations |
| Final report | 12 | System design & architecture, Implementation explanation, Evaluation & performance, Academic writing |
| Teamwork & individual contributions | 2 | Individual contributions (0–1), Teamwork (0–1). Solo students are graded without the teamwork part |

Grade mapping: 5 = 24–26, 4 = 19–23, 3 = 15–18, 2 = 11–14, 1 = 1–10, 0 = 0.

### What "Exceptional" requires (target level)
- **Functionality:** correct EMA38 and EMA100 per window, and correct buy/sell advisories for all subscribed symbols.
- **Performance:** the entire week processed smoothly, with high throughput and low, stable latency.
- **Scalability:** all exchanges and volumes, including peak rates and the long-tail distribution, with capacity for further increases.
- **Creative explorations:** design space exploration backed by measurements, and/or a smart visualization.
- **Report, design:** handles CEP, real-time requirements, and scalability.
- **Report, implementation:** covers event handling, windowing and EMA state, crossover detection, normalization, and metadata usage.
- **Report, evaluation:** correctness, throughput and latency under load, scalability across exchanges and resources (CPU/GPU utilization), and long-tail handling.
- **Report, writing:** professional and clear, with a strong dataset analysis, following the template and page limit.

---

## 7. Open Questions / Ambiguities to Resolve

- **Empty windows:** the spec does not say how EMA is updated when a symbol has no price event in a window. Options: carry the previous EMA forward, reuse the last known close, or skip the window. Decide and document.
- **Event time vs. arrival order:** which timestamp defines window membership (`Trading time` + `Trading date`, or system `Date` + `Time`)? The spec marks `Trading time` and `Trading date` as relevant.
- **Late-event policy:** how long to wait for out-of-order events before evaluating a window, and what to do with events that arrive after evaluation.
- **Subscription mechanism:** how the subscribed subset `S'` is defined or changed (static config vs. dynamic).
- **Output format and sink** for EMA results and advisories.
- **Typo in spec:** section 3.3.2 says "For the bullish pattern … create a sell advise event". From context this is the *bearish* pattern.
- **Initialization artifacts:** the `EMA = 0` initialization means early crossovers right after a symbol's first price are artifacts of initialization. Consider whether to flag them. The spec's formulas still apply as written.
