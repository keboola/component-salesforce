**0.2.0**

- add optional `polymorphic_replace_string` config field for colon replacement in column names, enabling Salesforce polymorphic relationship fields (e.g. `Lead:Who.Email`)
- reject overlapping `replace_string` / `polymorphic_replace_string` sentinels with a clear error
- add unit tests for header replacement logic

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
