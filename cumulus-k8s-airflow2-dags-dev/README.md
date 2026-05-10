# cumulus-k8s-airflow2-dags
Airflow 2.0 dag repo for Cumulus pipelines.

## Documentation Links

Wiki page: https://rndwiki.inc.hpicorp.net/confluence/display/BigData/Airflow+2.0

Ops Docs: https://pages.github.azc.ext.hp.com/hp-data-platform/dataos-ops-docs/#_airflow_2_0

## Airflow Instances

_To get access to an instance, we need to add the user to the proper role in the airflow helm releases and 
create a PR for the Ops team. Please create a ticket with the ops team or reach out to Ryan or Isaac for 
assistance with this if needed._

| Instance | Branch | URL |
|----------|--------|-----|
| Dev      | [dev](https://github.azc.ext.hp.com/cirrostratus/cumulus-k8s-airflow2-dags/tree/dev)    | [https://airflow.dev.hpdataos.com](https://airflow.dev.hpdataos.com)    |
| Stage    | [itg](https://github.azc.ext.hp.com/cirrostratus/cumulus-k8s-airflow2-dags/tree/itg)    | [https://airflow.stg.hpdataos.com/](https://airflow.stg.hpdataos.com/)    |
| Prod     | [master](https://github.azc.ext.hp.com/cirrostratus/cumulus-k8s-airflow2-dags/tree/master) | [https://airflow.hpdataos.com/](https://airflow.hpdataos.com/)    |

## Promotion

There is a codeway pipeline set up in Azure Devops to promote dags and variables to each branch/airflow 
instance. There is additionally a pipeline for promoting images from core-dev ECR to the core-prod ECR.

#### Promotion Pipelines:
- [Dag/Variable Promotion](https://dev.azure.com/hpcodeway/dataos/_build?definitionId=5592)
- [Dag/Variable Configuration Repo](https://github.azc.ext.hp.com/cirrostratus/airflow2-promotion)
- [Image Promotion Pipeline](https://dev.azure.com/hpcodeway/dataos/_build?definitionId=3350)
- [Image Promotion Configuration File](https://github.azc.ext.hp.com/hp-data-platform/k8s-dataos-core-prod/blob/master/versions/versionsets/cumulus_airflow_base.yml)

