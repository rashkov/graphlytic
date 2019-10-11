#!/usr/bin/env python
from random import randint
from flask import Flask, render_template, request
from wtforms import Form, TextField, TextAreaField, validators, StringField, SubmitField
import redis
import requests

app = Flask(__name__)
app.config.from_object(__name__)


class ReusableForm(Form):
  term = TextField('Term:', validators=[validators.DataRequired()])

@app.route("/", methods=['GET', 'POST'])
def hello():
  form = ReusableForm(request.form)
  extracts = None
  if request.method == 'POST' and form.validate():
    terms=request.form['term']
    matchingArticleIds = search(terms)
    extracts = fetchExtracts(matchingArticleIds)
  return render_template('index.html', form=form, extracts=extracts)

def fetchExtracts(ids):
  if not ids:
    return []
  # Fetch a URL in this style:
  # https://en.wikipedia.org/w/api.php?format=json&action=query&prop=extracts&exsentences=10&exintro=true&explaintext=true&pageids=15646344|1133422|552812
  extracts = []
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
    extracts.append({ 'title': title, 'extract': extract, 'pageid': pageId, 'title_url': str.lower(title).replace(" ", "_") })
  return extracts

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

if __name__ == "__main__":
    app.run()
