# Vermont Covid-19 Reporting Charts with Prefect

A rewrite of my [Vermont Covid-19 Jupyter Notebook](https://github.com/tpdorsey/vt-covid-jupyter-charts) as a [Prefect](https://prefect.io) task flow.

The flow creates three charts based on Vermont Covid-19 case reporting and saves them as .png files:

- New cases and related data with a rolling 7-day average.
- New cases charted along with an estimate of current active infection, using a sum of trailing new cases.
- Current hospitalizations including ICU.

By default the code is configured to run as a local flow and to save the files in the current directory, but you can specify a location to save the files and uncomment the `flow.register()` line to register with a backend project.

![Screenshot showing an example of the active cases chart](https://terrencedorsey.com/files/vtc/vt-covid-active-cases.png)