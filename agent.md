- we work with python, docker and sqlalchemy for orm
- we always write tests first, and then the code never change tests to pass without explicity asking for it
- we have env file for configuration
- for portability, we use docker and sqlite
- the workers are in the docker container
- also the management of the system is in the docker container or with database triggers
