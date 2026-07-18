# Validation summary

- Rows: 100
- Regions: 5
- Tower IDs: 8
- Required-field nulls: 0
- Duplicate provisional keys: 0
- Invalid signal values: 0
- Negative data-volume values: 0
- Event-date/filename-date warnings: 4

The four warning records belong to 2025-07-24 although the file is named for 2025-07-23. They are retained and Gold is rebuilt for every affected event date.

Historical anomaly scoring executes successfully and reports
`INSUFFICIENT_HISTORY` for this one-day sample. It automatically becomes eligible
after seven observations exist within the configured 28-day region/hour baseline.

`validation_log_sample.jsonl` also contains one clearly labelled synthetic
failure from a separate illustrative run. It demonstrates that a threshold
breach quarantines invalid rows, blocks Silver promotion and fails the pipeline;
it is not part of the successful 100-row sample result above.
