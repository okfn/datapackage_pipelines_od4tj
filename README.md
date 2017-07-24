# Open Data for Tax Justice Pipelines and Processors

The purpose of this pipeline is to collect and normalize all CRD IV data into a database.

Currently we expect the data source to be PDF tables. Please open an issue to request support for other formats.

## How to contribute a new dataset

1. Check if the entity (e.g. bank, company) already has [a folder in our pipeline](https://github.com/okfn/datapackage_pipelines_od4tj/tree/master/datapackage_pipelines_od4tj/pipelines/crd-iv).

2. If it doesn't, create a new folder for the entity (e.g. bank, company) that generated the dataset.

3. Add the PDF to the folder.

4. Open the PDF in [Tabula](http://tabula.technology/).

5. Select the table and twitch settings (e.g. `stream/lattice`) until the parsed table contains all the information it should.

    Do not worry if the parsed table contains extra rows: e.g. header row is split into multiple rows; there are category rows that doesn't contain data. Those will be eliminated in the pipeline.

    The most important aspect is for the parsed table to contain all the selected information.

6. Export the selection dimensions by following these steps in Tabula:

  Export -> JSON (dimensions) -> Copy to clipboard -> Paste where you need it

7. If it doesn't exist create a file named `od4tj.source-spec.yaml` in the folder of your entity.

8. Append the following template to the end of the `od4tj.source-spec.yaml` file and replace with information about your dataset.

```yaml
-
  # Year
  year: 2014
  # EntityName
  entity: Barclays
  inputs:
    # Path to the PDF you uploaded in the project
    - url: ./2014_barclays_country_snapshot.pdf
      kind: pdf
      parameters:
        # Paste the dimensions of the selection you made in Tabula
        dimensions: [
                      {
                          "extraction_method": "stream",
                          "height": 446.26,
                          "page": 4,
                          "selection_id": "M1500294534993",
                          "spec_index": 0,
                          "width": 779.9025,
                          "x1": 33.15375,
                          "x2": 813.05625,
                          "y1": 65.78125,
                          "y2": 512.04125
                      },
                      {
                          "extraction_method": "stream",
                          "height": 446.26,
                          "page": 5,
                          "selection_id": "F1500294537181",
                          "spec_index": 1,
                          "width": 779.9025,
                          "x1": 33.15375,
                          "x2": 813.05625,
                          "y1": 65.78125,
                          "y2": 512.04125
                      },
                      {
                          "extraction_method": "stream",
                          "height": 303.12,
                          "page": 6,
                          "selection_id": "R1500294537195",
                          "spec_index": 2,
                          "width": 788.3225,
                          "x1": 33.15375,
                          "x2": 821.4762499999999,
                          "y1": 65.78125,
                          "y2": 368.90125
                      }
                    ]
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

9. Congrads, you created a new pipeline. Please open a Pull Request with your contribution.
