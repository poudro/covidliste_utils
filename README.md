# covidliste_utils

## extract_list.py (Python 3)

Used to extract volunteers data and pictures from CSV and slack and format them as a json compatible with Covidliste's website.

### Setup 

Copy initial configuration and tweak it
```sh
cp extract_list/config.py.sample extract_list/config.py
vim extract_list/config.py
```
- Twitter API token must be generated from https://developer.twitter.com/en/portal/dashboard (used to download avatars)
- Slack API token must be generated from https://api.slack.com/apps
- Slack Channel is the slack ID of the channel where all volunteers are present
- Volunteers CSV url is the direct url to the volunteers CSV. For a Google Sheet it will be something like https://docs.google.com/spreadsheets/d/XXXX/gviz/tq?tqx=out:csv&sheet=YYYY&headers=0

Install dependencies (use Python 3)
```sh
pip install -r extract_list/requirements.txt
```

You will need covidliste source code from https://github.com/hostolab/covidliste cloned elsewhere on the same machine.

### Usage

If `../covidliste` is where the covidlist repo has been cloned on the same machine, the command will be :
```sh
python3 extract_list/extract_list.py --out-json ../covidliste/db/frozen_records/volunteers.json --out-pics-folder ../covidliste/app/assets/images/volunteers
``
