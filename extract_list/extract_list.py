import argparse
import csv
import logging
from urllib.parse import urlparse
import requests
import json
import hashlib
import os
import re
from io import BytesIO
from PIL import Image
from resizeimage import resizeimage
from resizeimage.imageexceptions import ImageSizeError
from bs4 import BeautifulSoup
import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)d %(levelname)s:%(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger('extract_list')

pics_mimetypes = set(["image/png", "image/jpeg", "image/jpg", "image/gif"])

key_mappings = {
    'Nom complet': 'fullname',
    'Canal #bénévoles_général': 'canal',
    'Prénom 👀': 'firstname',
    'Nom 👀': 'lastname',
    'Identité 👀': 'identity',
    'Pseudo slack (si différent du nom complet)': 'nick',
    'Adresse mail (celle utilisée pour le slack)': 'email',
    'Téléphone portable (si numéro français, format français, sinon format international +32 XX...)': 'phone',
    "J'accepte d'être mentionné comme bénévole en public (site + twitter)": 'mention',
    'Votre équipe dans Covidliste': 'team',
    "L'équipe que vous leadez (si vous êtes lead)": 'leading_team',
    'Code postal de résidence': 'res_postcode',
    'Ville de résidence': 'res_city',
    "Code postal d'origine": 'orig_postcode',
    "Ville d'origine": 'orig_city',
    'GitHub (pseudo seulement) 👀': 'github',
    'Linkedin (lien du profil seulement) 👀': 'linkedin',
    'Twitter (pseudo seulement) 👀': 'twitter',
    'Autre pseudo (si vous voulez apparaitre sous un pseudo) 👀': 'nickname',
    'Mini bio 👀': 'bio',
    'Spécialité 👀': 'specialty',
    'Disponibilité': 'dispo',
    "Role dans Covidliste, ce que vous faites quoi (en plus de l'équipe) 👀": 'role',
    'Photo ou avatar sous forme de lien 👀': 'pic',
    "Commentaire autre, si vous ne voulez pas qu'on publie un truc, si vous avez autre chose à dire": 'comment',
}

public = set([x[1] for x in filter(lambda x: '👀' in x[0], key_mappings.items())] + ['picture', 'anon', 'id'])

default_headers = {
   'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.128 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
}

def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
                            "--csv",
                            type=str,
                            required=True,
                        )
    parser.add_argument(
                            "--out-json",
                            type=str,
                            required=True,
                        )
    parser.add_argument(
                            "--out-pics-folder",
                            type=str,
                            required=True,
                        )
    args, unknown = parser.parse_known_args()

    return args


def get_people(filename):
    people = []
    with open(filename) as f:
        r = csv.reader(f)
        headers = None
        for row in r:
            if not headers and row[0] == 'Nom complet':
                headers = row
            elif not list(filter(None, row)):
                break
            elif headers:
                peep = {key_mappings[k]: v for k, v in zip(headers, row)}
                person_id = hashlib.sha256(peep['email'].encode("utf-8")).hexdigest()
                person_id = hashlib.md5(person_id.encode("utf-8")).hexdigest()
                peep['id'] = person_id
                if peep['canal'] == 'Oui':
                    people.append(peep)

    return people


def handle_mention(peep):
    if peep['mention'] == 'Non' or peep['mention'] == '':
        for k, v in peep.items():
            if k not in ["id", "team", "leading_team"]:
                peep[k] = ""
        peep['anon'] = True
        return peep
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

    up = urlparse(src)
    if not up.netloc:
        return None

    if 'zupimages.net' in up.netloc and 'viewer.php' in up.path:
        src = 'https://www.zupimages.net/up/%s' % up.query.replace('id=', '')

    r = requests.get(src, headers=default_headers)
    if r.status_code >= 200 and r.status_code <= 209 and r.headers["content-type"] in pics_mimetypes:
        return src
    else:
        logger.warning(f"{peep['firstname']} {peep['lastname']} -> picture url does not point to a valid picture {src}")

    return None


def get_github_pic(peep):
    handle = peep['github']
    r = requests.get(f'https://github.com/{handle}', headers=default_headers)
    if r.status_code == 200:
        soup = BeautifulSoup(r.text, 'lxml')
        img = soup.select('img.avatar-user.width-full')
        if img:
            src = img[0].get('src')
            return src
    return None


def get_twitter_pic(peep):
    handle = peep['twitter']
    twitter_headers = default_headers;
    twitter_headers["authorization"] = "Bearer " + config.TWITTER_API_BEARER_TOKEN
    r = requests.get(f'https://api.twitter.com/1.1/users/show.json?screen_name={handle}', headers=twitter_headers)
    if r.status_code == 200:
        twitter_user = r.json()
        if twitter_user and not twitter_user["default_profile_image"] and twitter_user["profile_image_url_https"]:
            src = re.sub(r'_normal\.', '.', twitter_user["profile_image_url_https"])
            return src
    # no way to retreive it by scrapping now, we must use https://developer.twitter.com/en/docs/twitter-api/v1/accounts-and-users/user-profile-images-and-banners
    return None


def get_pic(peep, pics_folder):
    priority = ['pic', 'twitter', 'linkedin', 'github']
    for field in priority:
        if peep[field]:
            pic_name = None
            if field == 'pic':
                pic_name = download_and_crop_pic(peep, verify_pic(peep), pics_folder)
            elif field == 'github':
                pic_name = download_and_crop_pic(peep, get_github_pic(peep), pics_folder)
            elif field == 'twitter':
                pic_name = download_and_crop_pic(peep, get_twitter_pic(peep), pics_folder)

            if pic_name:
                return pic_name

    return None

def download_and_crop_pic(peep, pic_url, pics_folder):
  if not pic_url:
    return None
  image_name = "volunteer-" + peep['id']
  r = requests.get(pic_url, headers=default_headers)
  if r.status_code >= 200 and r.status_code <= 209 and r.headers["content-type"] in pics_mimetypes:
    with Image.open(BytesIO(r.content)) as image:
        image = image.convert('RGB')
        try:
          cover = resizeimage.resize_cover(image, [200, 200])
          cover.save(pics_folder + os.path.sep + image_name + '.jpg', image.format)
          return image_name+'.jpg'
        except ImageSizeError as e:
          logger.warning(f"{peep['firstname']} {peep['lastname']} -> picture cannot be cropped : {e.message} - {pic_url}")

def to_json(people, json_file, pics_folder):
    out = []
    for peep in people:
        peep = handle_mention(peep)

        if peep:
            if peep['linkedin']:
                peep['linkedin'] = re.sub(r'(https?://)?www\.linkedin\.com/', 'https://www.linkedin.com/', peep['linkedin'])
            if not peep['anon']:
                peep['picture'] = ''
                pic_name = get_pic(peep, pics_folder)
                if pic_name:
                  peep['picture'] = pic_name

            del peep['pic']
            out.append({k: v for k, v in peep.items() if k in public})

    with open(json_file, 'w') as f:
        json.dump(out, f, sort_keys=True, indent=2)


if __name__ == '__main__':
    args = get_args()
    people = get_people(args.csv)
    to_json(people, args.out_json, args.out_pics_folder)
