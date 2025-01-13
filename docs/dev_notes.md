# Potential future work:

IF useful to other teams we COULD:
    * deploy to the oracle struct to openshift for use by all teams.
    * wrap cli code with an api interface:
        * deploy db to openshift
        * create a simple api to query for dependencies.

    * could create a simple api that would allow the retrieval of dependencies
      and migrations files through an endpoint.

Other Topics
    * When developing migrations mistakes happen... I feel like having the
      rollbacks are a requirement for any database migration tool

    * Potential options
        * [golang-migrate](https://github.com/golang-migrate/migrate)
        * [atlas $$$](https://atlasgo.io/pricing) [github link](https://github.com/ariga/atlas)
        * [sqitch](https://sqitch.org/about/)

    [db migration tool comparison chart (from dbmate repo)](https://github.com/amacneil/dbmate?tab=readme-ov-file#alternatives)

