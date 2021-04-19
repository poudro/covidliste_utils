import argparse
import csv
import logging
from urllib.parse import urlparse
import requests
import json
from bs4 import BeautifulSoup


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)d %(levelname)s:%(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger('extract_pics')


key_mappings = {
    'Nom complet': 'fullname',
    'Canal #bénévoles_général': 'canal',
    'Prénom 👀': 'firstname',
    'Nom 👀': 'lastname',
    'Identité 👀': 'id',
    'Pseudo slack (si différent du nom complet)': 'nick',
    'Adresse mail': 'email',
    'Téléphone portable (si numéro français, format français, sinon format international +32 XX...)': 'phone',
    "J'accepte d'être mentionné comme bénévole en public (site + twitter)": 'mention',
    'Votre équipe dans Covidliste 👀': 'team',
    'Code postal de résidence': 'res_postcode',
    'Ville de résidence': 'res_city',
    "Code postal d'origine": 'orig_postcode',
    "Ville d'origine": 'orig_city',
    'GitHub (pseudo seulement) 👀': 'github',
    'Linkedin (lien du profil seulement) 👀': 'linkedin',
    'Twitter (pseudo seulement) 👀': 'twitter',
    'Autre pseudo (si vous voulez apparaitre sous un pseudo) 👀': 'other_nick',
    'Mini bio 👀': 'bio',
    'Spécialité 👀': 'specialty',
    'Disponibilité': 'dispo',
    "Role dans Covidliste, ce que vous faites quoi (en plus de l'équipe) 👀": 'role',
    'Photo ou avatar sous forme de lien 👀': 'pic',
    "Commentaire autre, si vous ne voulez pas qu'on publie un truc, si vous avez autre chose à dire": 'comment',
}

public = set([x[1] for x in filter(lambda x: '👀' in x[0], key_mappings.items())] + ['verified_pic', 'anon'])


def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
                            "--csv",
                            type=str,
                            required=True,
                        )
    parser.add_argument(
                            "--out",
                            type=str,
                            required=True,
                        )
    args, unknown = parser.parse_known_args()

    return args


def get_people(filename):
    people = []
    with open(args.csv) as f:
        r = csv.reader(f)
        headers = None
        for row in r:
            if not headers and row[0] == 'Nom complet':
                headers = row
            elif not list(filter(None, row)):
                break
            elif headers:
                peep = {key_mappings[k]: v for k, v in zip(headers, row)}
                if peep['canal'] == 'Oui':
                    people.append(peep)

    return people


def handle_mention(peep):
    if peep['mention'] == 'Non' or peep['mention'] == '':
        return {'anon': True}
    elif peep['mention'] == 'Oui : uniquement Prénom + 1ère lettre du Nom':
        peep['lastname'] = peep['lastname'][0]
    elif peep['mention'] == 'Oui : uniquement Prénom':
        peep['lastname'] = ''
    elif peep['mention'] == 'Oui : uniquement Autre Pseudo':
        peep['firstname'] = ''
        peep['lastname'] = ''
    elif peep['mention'] == 'Autre chose : précisez en commentaire':
        logger.warning(f"{peep['firstname']} {peep['lastname']} -> needs manual attention ({peep['comment']})")
        return None
    elif peep['mention'] == 'Oui : nom complet':
        # nothing to do
        pass

    peep['anon'] = False
    return peep


def verify_pic(peep):
    src = peep['pic']
    if not src:
        return None

    mimetypes = set(["image/png", "image/jpeg", "image/jpg", "image/gif"])

    up = urlparse(src)
    if not up.netloc:
        return None

    if 'zupimages.net' in up.netloc and 'viewer.php' in up.path:
        src = 'https://www.zupimages.net/up/%s' % up.query.replace('id=', '')

    r = requests.head(src)
    if r.status_code >= 200 and r.status_code <= 209 and r.headers["content-type"] in mimetypes:
        return src
    else:
        logger.warning(f"{peep['firstname']} {peep['lastname']} -> picture url does not point to a valid picture {src}")

    return None


def get_github_pic(peep):
    handle = peep['github']
    r = requests.get(f'https://github.com/{handle}')
    if r.status_code == 200:
        soup = BeautifulSoup(r.text, 'lxml')
        img = soup.select('img.avatar-user.width-full')
        if img:
            src = img[0].get('src')
            return src
    return None


# def get_twitter_pic(peep):
#     handle = peep['twitter']
#     r = requests.get(f'https://twitter.com/{handle}/photo')
#     if r.status_code == 200:
#         soup = BeautifulSoup(r.text, 'lxml')
#     return None


def get_pic(peep):
    priority = ['pic', 'github', 'twitter', 'linkedin']
    for field in priority:
        if peep[field]:
            if field == 'pic':
                src = verify_pic(peep)
            elif field == 'github':
                src = get_github_pic(peep)
            # elif field == 'twitter':
            #     src = get_twitter_pic(peep)

            if src:
                return src

    return None


def to_json(people, json_file):
    out = []
    for peep in people:
        peep = handle_mention(peep)

        if peep:
            if not peep['anon']:
                pic = get_pic(peep)
                if pic:
                    peep['verified_pic'] = pic
                else:
                    peep['verified_pic'] = ''

            out.append({k: v for k, v in peep.items() if k in public})

    with open(json_file, 'w') as f:
        json.dump(out, f)


if __name__ == '__main__':
    args = get_args()
    people = get_people(args.csv)
    to_json(people, args.out)
