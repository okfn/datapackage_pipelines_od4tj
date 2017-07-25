# Open Data for Tax Justice Pipelines and Processors

The purpose of this pipeline is to collect and normalize all CRD IV data into a database.

Currently we expect the data source to be PDF tables. Please open an issue to request support for other formats.

## How to contribute a new dataset

1. Download the file (but write down the download link)

2. Open the PDF in your local installation of  [Tabula](http://tabula.technology/).
   Alternatively, you can use the online version at [https://tabula.openknowledge.io](https://tabula.openknowledge.io)

3. your stage 5 & 6
Email us (or open an issue) with the following information:
Download link
Company
Year
Dimension
Column mapping
Currency & units (e.g. millions)


3. Select the table and twitch settings (e.g. `stream/lattice`) until the parsed table contains all the information it should.

    Do not worry if the parsed table contains extra rows: e.g. header row is split into multiple rows; there are category rows that doesn't contain data. Those will be eliminated in the pipeline.

    The most important aspect is for the parsed table to contain all the selected information.

4. Export the selection dimensions by following these steps in Tabula:

  Export -> JSON (dimensions) -> Copy to clipboard -> Paste where you need it
  
5. Open an Issue in Github and add all this information:
 - Download link
 - Company
 - Year
 - Dimension
 - Column mapping
 - Currency & units (e.g. millions)
 
 And any other information that might be relevant

