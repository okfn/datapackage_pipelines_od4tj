```yaml
-
  # Year
  year: 2014
  # EntityName
  entity: Barclays
  inputs:
    # Path to the PDF you uploaded in the project
    - url: https://download-rl-of-the-file
      kind: pdf
      parameters:
        # Paste the dimensions of the selection you made in Tabula
        dimensions: <paste-here>
  processing: {}
  model:
    # Define the fields of the table in order they appear in the table
    headers:
      # `string` type is default
      - name: country
      - name: commentary
      # `number` type if it's a number with decimal points
      # `currency` in [ISO_4217](https://en.wikipedia.org/wiki/ISO_4217)
      # `factor` of the amount (e.g. 5.3 factor 1 million = 5300000)
      - name: turnover
        type: number
        currency: gbp
        factor: 1m
      - name: profit_before_tax
        type: number
        currency: gbp
        factor: 1m
      - name: total_tax_paid
        type: number
        currency: gbp
        factor: 1m
      - name: subsidies
        type: number
        currency: gbp
        factor: 1m
      - name: full_time_equivalents
        type: number

```
