**1.0.5**

- deprecate the "Security token with username and password" login method: Salesforce retires the SOAP
  API login() call in the Summer '27 release. The component now writes a warning and the documentation
  describes the migration to the connected app client credentials flow.
- raise the API version fallback from 40.0 to 64.0 for configurations with no api_version set
- replace a stored API version below 31.0 (retired by Salesforce) with the fallback, and log the replacement
- fix the release pipeline: a semantic tag on the default branch was not deployed when another branch
  also contained the tagged commit

**0.1.1**

- fix requirements
- add src folder to path for tests

**0.1.0**

- src folder structure
- remove dependency on handler lib - import the code directly to enable modifications until its released

**0.0.2**

- add dependency to base lib
- basic tests

**0.0.1**

- add utils scripts
- move kbc tests directly to pipelines file
- use uptodate base docker image
- add changelog
