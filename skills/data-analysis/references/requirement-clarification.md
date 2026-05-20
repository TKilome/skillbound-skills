# Requirement Clarification

Use this reference before SQL analysis when the user's data request is
ambiguous, incomplete, or could map to multiple business definitions.

## Internal Requirement State

Maintain this state internally:

- `business_goal`: question the user wants the data to answer.
- `analysis_type`: `detail` or `summary`.
- `business_object`: business object or domain, such as order, refund, user,
  course, teacher, channel, lead, or inventory.
- `metrics`: measures such as amount, count, rate, conversion, duration, or GMV.
- `dimensions`: grouping fields such as channel, course, product, region, owner,
  status, or user segment.
- `filters`: business filters such as status, product category, channel, or
  organization scope.
- `time_field`: date field used for filtering, such as created time, paid time,
  completed time, refund time, updated time, or partition date.
- `time_range`: date range, such as yesterday, last 7 days, natural month, or
  explicit start/end.
- `output_fields`: required fields for detail output.
- `limit_sort`: row count and ordering for detail output.
- `missing_slots`: slots still needed.
- `next_action`: `ask_clarification`, `data_exploration`, `sql_analysis`, or
  `direct_answer`.

## Clarification Order

1. If the user gives a vague business topic and available data scopes are
   unknown, run lightweight `data_exploration` first.
2. Convert exploration results to business language before asking the user. Do
   not present raw table names as the primary choice.
3. If multiple business scopes exist, first ask which scope the user wants.
4. If the shape is unclear, ask whether they need detail rows or a summary.
5. Always clarify `time_field` and `time_range` before SQL analysis.
6. For `detail`, ask for `output_fields` if missing.
7. For `summary`, ask for `metrics` and `dimensions` if missing. Suggestions
   must come from discovered fields or known business catalog entries.
8. Ask only one or two related slots per user turn.

## Example Questions

Multiple business scopes:

```text
I found several order-related data scopes:
1. Transaction order detail for order amount, status, source, and owner.
2. Refund or after-sales orders for refund state, amount, and reason.
3. Course or product orders for course, product, or class-package analysis.

Which scope do you want to use? If you are unsure, I can continue with
transaction order detail.
```

Detail or summary:

```text
Do you need detail rows or a summary?
Detail means one row per record. Summary means aggregating metrics by dimension.
```

Time scope:

```text
Which time field and range should be used?
For example: created time for the last 7 days, completed time for yesterday, or
partition date for a specific day.
```

Detail output:

```text
Which fields should be included?
For example: basic information, status, amount, source channel, owner, or all
common fields.
```

Summary dimensions and metrics:

```text
Which dimensions and metrics should be used?
For example: group by channel, course, product, region, owner, or status; count
orders, sum order amount, sum paid amount, sum refund amount, or compute
conversion rate.
```
