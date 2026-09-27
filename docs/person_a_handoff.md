\# Person A — Implementation and Handoff



\## 1. Person A Role



Person A is responsible for the historical GDELT data layer:



1\. Data acquisition

2\. Parsing and processing

3\. Country filtering

4\. Events, Mentions and GKG preparation

5\. Events–Mentions–GKG joining

6\. Validation

7\. Temporal checks

8\. Preparing the intermediate data for Person B



The output of Person A is an intermediate analytical dataset. It is not yet the final country-day ML feature table.



\---



\## 2. Historical Scope



Current historical experiment:



\- Start: 2026-04-01

\- End: 2026-06-30

\- Duration: 91 days

\- Countries:

&#x20; - US — United States

&#x20; - IN — India

&#x20; - NI — Nigeria

&#x20; - UP — Ukraine

&#x20; - SF — South Africa



\---



\## 3. GDELT Data Layers



\### Events



The Events layer contains structured event records extracted from news.



The validated historical Events schema contains 58 columns.



Important fields used in the final daily join include:



\- GlobalEventID

\- Day

\- EventCode

\- EventRootCode

\- QuadClass

\- GoldsteinScale

\- NumMentions

\- NumSources

\- NumArticles

\- AvgTone

\- ActionGeo\_CountryCode

\- ActionGeo\_FullName

\- SOURCEURL



\### Mentions



Mentions connect GDELT events to individual news mentions.



Important fields used in the final join include:



\- GlobalEventID

\- MentionIdentifier

\- MentionTimeDate

\- MentionSourceName

\- Confidence

\- MentionDocLen

\- MentionDocTone



\### GKG



GKG provides additional document-level information.



The compact country-oriented representation contains 11 columns:



\- gkg\_record\_id

\- date

\- source\_common\_name

\- document\_identifier

\- v1\_5\_tone

\- v2\_gcam

\- v2\_enhanced\_themes

\- v2\_enhanced\_locations

\- v2\_1\_enhanced\_dates

\- gkg\_country\_codes

\- gkg\_country\_names



\---



\## 4. Events Processing



For April–June 2026:



\- 91 daily partitions

\- 9,016,776 total Event rows

\- 3,803,889 target-country Event rows

\- Target-country share: approximately 42.19%



Target-country Event counts:



| Country | Events |

|---|---:|

| IN | 461,474 |

| NI | 307,595 |

| SF | 99,448 |

| UP | 120,633 |

| US | 2,814,739 |



Main scripts:



\- `processing/person\_a/process\_events\_all.py`

\- `processing/person\_a/validate\_events\_parquet.py`



\---



\## 5. Compact GKG Processing



The original GKG data is very large.



A compact country-oriented representation was produced to reduce storage requirements.



For April–June 2026:



\- 8,735 timestamp folders

\- 4,767,103 compact GKG rows

\- Date range: `20260401000000` to `20260630234500`

\- No null GKG record IDs

\- No null dates

\- No null document identifiers

\- No null GKG country codes



Target-country counts:



| Country | GKG rows |

|---|---:|

| IN | 705,198 |

| NI | 138,072 |

| SF | 119,965 |

| UP | 169,087 |

| US | 3,942,908 |



Main scripts:



\- `processing/person\_a/process\_gkg\_all.py`

\- `processing/person\_a/filter\_gkg\_countries.py`

\- `processing/person\_a/validate\_gkg\_all.py`

\- `processing/person\_a/validate\_gkg\_countries.py`



The full processed GKG representation was removed after validation to conserve local disk space. The compact country-oriented representation was retained locally.



\---



\## 6. Mentions Processing



For April–June 2026:



\- 8,735 timestamp folders

\- Approximately 25.47 million rows

\- 16 columns



Validation confirmed:



\- GlobalEventID null count = 0

\- MentionTimeDate covers the intended period



Main scripts:



\- `processing/person\_a/process\_mentions\_all.py`

\- `processing/person\_a/validate\_mentions\_parquet.py`



\---



\## 7. Events → Mentions Coverage



For target-country Events:



\- Target Event IDs: 3,803,889

\- Distinct Mention Event IDs: 9,347,466

\- Target Event IDs with Mentions: 3,803,889

\- Target Event IDs without Mentions: 0

\- Coverage: 100%



This indicates that every target-country Event ID examined had at least one corresponding Mention.



Diagnostic scripts:



\- `processing/person\_a/diagnose\_events\_mentions\_coverage.py`

\- `processing/person\_a/check\_join\_size.py`



\---



\## 8. Mentions → GKG Matching



Target-country Mentions were compared with GKG document identifiers.



Results:



\- Target Mention rows: 9,620,114

\- Unique target Mention URLs: 1,913,911

\- Unique GKG document URLs: 4,767,103

\- Matching target URLs: 1,896,352

\- Unmatched target URLs: 17,559

\- Unique URL match rate: 99.08%



June 30 Stage 5 audit:



\- Joined rows: 120,174

\- GKG matched: 119,407

\- GKG unmatched: 767

\- Row-level GKG match rate: 99.36%



The final GKG join remains a LEFT JOIN so missing supplementary GKG metadata does not discard the underlying Event/Mention observation.



Diagnostic scripts:



\- `processing/person\_a/check\_mentions\_gkg\_url\_match.py`

\- `processing/person\_a/diagnose\_target\_mentions\_gkg\_coverage.py`



\---



\## 9. Stage 5 Daily Join



Main implementation:



`processing/person\_a/stage5\_daily\_join.py`



Processing is performed one day at a time.



For each day:



1\. Read the corresponding Events partition.

2\. Read the 96 Mentions timestamp partitions for that day.

3\. Read the 96 GKG timestamp partitions for that day.

4\. Restrict observations to the five target countries.

5\. Join Events with Mentions using `GlobalEventID`.

6\. Join Mentions with GKG using `MentionIdentifier` / `document\_identifier`.

7\. Write the daily joined Parquet output.



Local output:



`data/processed/person\_a/joined\_daily/YYYY-MM-DD`



All 91 days from April 1 through June 30 were successfully processed.



\---



\## 10. Temporal Audit



The Stage 5 implementation reads the Events partition corresponding to the processing day.



Mentions and GKG are explicitly restricted to timestamp folders belonging to the processing day.



An audit found that some Event partitions contain older Event `Day` values. This is not automatically leakage because historical events can appear in a current GDELT partition.



Three representative partitions were checked:



\- 2026-04-01

\- 2026-05-15

\- 2026-06-30



Future-dated Events (`Day > partition date`) were not observed in these audited partitions.



Important wording:



> No future-dated Events were observed in the three audited partitions.



This should not be interpreted as an exhaustive manual audit of every row across all 91 partitions.



No Stage 5 rebuild was required from this audit.



\---



\## 11. MentionType Audit



The Stage 5 implementation does not apply an additional `MentionType == WEB` filter.



Sampled processed Mentions from April, May and June showed:



`MentionType = 1`



for the examined batches.



This was a sampled validation, not an exhaustive audit of all 8,735 timestamp folders.



\---



\## 12. Join Cardinality



The June 30 audit showed:



\- Joined rows: 120,174

\- Distinct MentionIdentifier: 24,373

\- Repeated MentionIdentifier values: 18,972



Repeated MentionIdentifier values are expected because a URL can be associated with multiple mentions/events.



Earlier validation showed no duplicate GKG document URLs in the compact GKG representation.



The join did not show evidence of uncontrolled GKG-driven fan-out.



\---



\## 13. Unmatched GKG Investigation



The 767 unmatched GKG rows on June 30 were investigated.



The unmatched URLs were valid-looking news URLs rather than null identifiers.



The unmatched rows were distributed across many 15-minute intervals and many sources rather than being concentrated in one source.



Current decision:



\- retain the LEFT JOIN

\- keep the underlying Event/Mention observation

\- treat missing GKG fields as missing supplementary metadata



Person B should consider this when designing feature missingness handling.



\---



\## 14. Person A Output for Person B



Person B should use:



`data/processed/person\_a/joined\_daily/`



as the intermediate analytical input.



Person B should NOT treat each joined row as a final ML sample.



The next transformation is:



```text

Event/Mention/GKG observations

&#x20;       ↓

country-day aggregation

&#x20;       ↓

features

&#x20;       ↓

future independent outcome

&#x20;       ↓

escalation label

&#x20;       ↓

time-aware model evaluation

