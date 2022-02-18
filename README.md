# otl_interpreter

Otl interpreter for platform architecture 2.0. Plugin for [complex rest](https://github.com/ISGNeuroTeam/complex_rest/tree/develop)

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

###  Prerequisites
1. [Miniconda](https://docs.conda.io/en/latest/miniconda.html)
2. [Conda-Pack](https://conda.github.io/conda-pack)

### Deploy
1. Clone [complex rest](https://github.com/ISGNeuroTeam/complex_rest/tree/develop)
```bash
git clone git@github.com:ISGNeuroTeam/complex_rest.git
cd ./complex_rest
git checkout develop
```
2. Make `plugin_dev` and `plugins_dir`
```bash
mkdir plugins
mkdir plugin_dev
```
3. Clone otl_interpreter in `plugin_dev`
```bash
cd ./plugin_dev
git clone git@github.com:ISGNeuroTeam/otl_interpreter.git
```
4. Create python virtual environment
```bash
cd ./plugin_dev/otl_interpreter/otl_interpreter/
conda create -p ./venv
conda install -p ./venv python==3.9.7 -y
conda activate ./venv
pip install -r ../requirements.txt
```
5. Create symlink to otl_interpreter in `plugins` directory
```bash
cd ./plugins
ln -r -s ../plugin_dev/otl_interpreter/otl_interpreter otl_interpreter
```
6. Create configuration file
```bash
cd ./plugin_dev/otl_interpreter/otl_interpreter
cp otl_interpreter.conf.example otl_interpreter.conf
```
7. Create complex rest development environment in complex_rest directory
```bash
make dev
```
8. Launch complex_rest
```bash
./start.sh
```
9. Create database for otl_interpreter. Connect to postgres. User: `postgres`. Password: `postgres`
```bash
psql -U postgres -h localhost -p 5433
```
```postgresql
create role otl_interpreter with password 'otl_interpreter';
alter role otl_interpreter with login;
create database otl_interpreter;
grant all privileges on database otl_interpreter to otl_interpreter;
```
10. Make migrations and migrate them to database
```bash
python ./complex_rest/manage.py makemigrations otl_interpreter
python ./complex_rest/manage.py migrate otl_interpreter --database otl_interpreter
```

11. Checkout:  
- http://localhost:8080/admin/  
- http://localhost:8080/otl_interpreter/v1/checkjob/


## Running the tests
### Run tests in docker  
```bash
./plugin_dev/otl_interpreter/docs/scripts/run_tests_in_docker.sh
```
### Run tests
1. Launch complex_rest environment
```bash
./start.sh
```
2. Stop dispatcher process
```bash
source ./venv/bin/activate
supervisorctl stop dispatcher
```
3. Run tests
```bash
python ./complex_rest/manage.py test ./plugin_dev/otl_interpreter/tests --settings=core.settings.test
```


## Deployment

* Make plugin archive:
```bash
make pack
```
* Unpack archive into complex_rest plugins directory
* Create database for otl_interpreter and make migrations
* Configure otl_interpreter

## Built With

* [Django](https://docs.djangoproject.com/en/3.2.7/) - The web framework used


## Contributing

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/your/project/tags). 

## Authors


## License

[OT.PLATFORM. License agreement.](LICENSE.md)

## Acknowledgments

* Hat tip to anyone whose code was used
* Inspiration
* etc