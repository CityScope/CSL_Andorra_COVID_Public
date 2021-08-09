# Analysis
This directory contains the following analysis notebooks

## building_areas.ipynb
Computes the fraction of each H3 cell (at resolution 11) which is built-up. The results are saved as a json file (outputs/h3_res11_builtup.json) and used in later analysis.

## pre_lockdown_mobility.ipynb
Analysis of tourism in the period of Jan to Feb 2020 (immediately before lockdowns began). Includes computation of the Cross Crowding Index (CCI) between residents of each parish and tourists. The data inputs are sensitive and not publicly accessible.

## serology_infer_cases_bayes.ipynb
Estimation of the total number of COVID-19 exposures as of the beginning of the masss serology screening in May 2020. Results are tabulated by parish of residence and by residency status. The data inputs are sensitive and not publicly accessible.

## colocations_and_crowding.ipynb
Computation of the colocations and indoor/outdoor crowding index for every spatio-temporal cell in Andorra during the study-period. The data inputs are sensitive and not publicly accessible.

## cases_vs_mobility.ipynb
Computation of case growth rate over the susceptible population, as well as computation of multiple mobility metrics. The susceptible population used in the case growth rate computation is inferred from the precomputed presence data.
Correlations between the various mobility metrics and case growth are compared.

## staying_home.ipynb
Computation and analysis of the number of users staying home, by parish, over the timeline of the study.
