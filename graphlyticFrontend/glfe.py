#!/usr/bin/env python
from random import randint
from flask import Flask, render_template, request
from wtforms import Form, TextField, validators
import redis
import requests
import os

app = Flask(__name__)
app.config.from_object(__name__)

class ReusableForm(Form):
  term = TextField('Term:', validators=[validators.DataRequired()])

@app.route("/", methods=['GET', 'POST'])
def queryGraphlytic():
  form = ReusableForm(request.form)
  extracts = None
  if request.method == 'POST' and form.validate():
    terms=request.form['term']
    matchingArticleIds = search(terms)
    extracts = fetchExtracts(matchingArticleIds, terms)
  return render_template('index.html', form=form, extracts=extracts)

def fetchExtracts(ids, terms):
  if not ids:
    return []
  # Fetch a URL in this style:
  # https://en.wikipedia.org/w/api.php?format=json&action=query&prop=extracts&exsentences=10&exintro=true&explaintext=true&pageids=15646344|1133422|552812
  extracts = []
  titleMatch = []
  articleMatch = []
  params={
    'action': "query",
    'format': "json",
    'prop': "extracts",
    'exsentences': 10,
    'exintro': True,
    'explaintext': True,
    'pageids': "|".join([str(i) for i in ids[:20]])
  }
  respJSON = requests.get(url="https://en.wikipedia.org/w/api.php", params=params).json()
  pages = respJSON['query']['pages']
  for pageId in pages:
    [title, extract] = [pages[pageId].get(key) for key in ['title', 'extract']]
    result = { 'title': title, 'extract': extract, 'pageid': pageId, 'title_url': str.lower(title).replace(" ", "_") }
    if overlaps(title, terms):
      titleMatch.append(result)
    elif overlaps(title, extract):
      articleMatch.append(result)
    else:
      extracts.append(result)

  # Boost results which match the title, followed by those matching the extract
  results = titleMatch
  results.extend(articleMatch)
  results.extend(extracts)
  return results

def search(terms):
  try:
    r = redis.Redis(
        host=os.environ['REDIS_IP'],
        port=os.environ['REDIS_PORT'],
    )
    results = [int(a.decode()) for a in r.sinter(terms.split(' '))]
    results.sort()
    return results
  except:
    return None
  else:
    r.close()

def overlaps(str1, str2):
  str1 = str1.lower()
  str2 = str2.lower()
  return len(set(str1.split(" ")) & set(str2.split(" "))) > 0

if __name__ == "__main__":
    app.run()
