#!/usr/bin/env python
#
# Copyright 2007 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import wsgiref.handlers
import cgi

from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.api import users


class StringKeyValue(db.Model):
  k = db.StringProperty()
  v = db.StringProperty()

class MainFrameHandler(webapp.RequestHandler):
  def get(self):
    user = users.get_current_user()
    if not user:
      self.redirect(users.create_login_url("/"))
      return

    self.response.out.write("""
      <frameset rows="75%,25%">
      <frame src="/main" name="main">
      <frame src="/ready" name="status">
      </frameset>""")

class MainHandler(webapp.RequestHandler):
  def get(self):
    if not users.get_current_user():
      self.redirect(users.create_login_url("/"))
      return

    logout_url = users.create_logout_url(self.request.uri)
    self.response.out.write("MapReduce on Google App Engine Demo<br>Please do not try too much to save the creator... :) <a href='%s' target='_top'><font align='right'>Logout</a></font>" % logout_url)

    self.response.out.write("""
    <p>Press "Run Map" button and wait for all frames to be green, and click "Run Shuffle & Reduce" button to get final results in the frames.</p>
    <form action="/runmap" method="get" target="status">
    <div>Input<br><textarea name="input" rows="1" cols="100">["This is a pen", "That is a pen too", "That is your pen", "This is my pen", "This is pencil"]</textarea></div><br>
    <div>Map Code:<br><textarea name="map_code" rows="3" cols="60">dict(map(lambda x: (x,'1'), input.split(' ')))</textarea></div>
    <div><input type="submit" value="Run Map"></div>
    </form>
    <form action="/runreduce" method="get" target="status">
    <div>Reduce Code:<br><textarea name="reduce_code" rows="3" cols="60">str(len(values))</textarea></div>
    <div><input type="submit" value="Run Shuffle & Reduce"></div>
    </form>""")

class ReadyHandler(webapp.RequestHandler):
  def get(self):
    if not users.get_current_user():
      return

    self.response.out.write("Ready.")

class RunMapHandler(webapp.RequestHandler):
  def get(self):
    if not users.get_current_user():
      return

    # clear temporary data
    q = StringKeyValue.all()
    results = q.fetch(1000)
    for result in results:
      result.delete()

    input = eval(self.request.get('input'))
    map_code = self.request.get('map_code')

    # mappers
    num_mapper = len(input)
    map_frame_size_str = (str(100/num_mapper)+ "%,") * num_mapper
    
    self.response.out.write("<frameset cols='%s'>" % map_frame_size_str)
    for i in range(num_mapper):
      self.response.out.write("<frame src=\"/map?code=%s&input=%s\">" % (cgi.escape(map_code), cgi.escape(input[i])))
    self.response.out.write("</frameset>")

class RunReduceHandler(webapp.RequestHandler):
  def get(self):
    if not users.get_current_user():
      return

    reduce_code = self.request.get('reduce_code')

    # reducers
    num_reducer = 10
    map_frame_size_str = (str(100/num_reducer)+ "%,") * num_reducer
    self.response.out.write("<frameset cols='%s'>" % map_frame_size_str)
    for i in range(num_reducer):
      self.response.out.write("<frame src=\"/reduce?code=%s&id=%d&num=%d\">" % (reduce_code, i, num_reducer))
    self.response.out.write("</frameset>")

class MapHandler(webapp.RequestHandler):
  def get(self):
    if not users.get_current_user():
      return

    input = self.request.get('input')
    code = self.request.get('code')
    output = eval(code) # expect dictionary of key,value pairs
    
    for (k,v) in output.items():
      # self.response.out.write("%s  %s<br>" % (k,v))
      o = StringKeyValue()
      o.k = k
      o.v = v
      o.put()

    self.response.out.write("<body bgcolor='green'></body>")

class ReduceHandler(webapp.RequestHandler):
  def get(self):
    if not users.get_current_user():
      return

    code = self.request.get('code')
    reducer_id = int(self.request.get('id'))
    num_reducer = int(self.request.get('num'))

    q = StringKeyValue.all()
    inputs = q.fetch(1000)

    # shuffle
    key_to_values = {}
    for input in inputs:
      k = input.k
      v = input.v
      if hash(k) % num_reducer == reducer_id:
        if k not in key_to_values:
          key_to_values[k] = []
        key_to_values[k].append(v)

    # reduce
    for kv in key_to_values.items():
      (key, values) = kv
      output = eval(code) # expects list of strings
      self.response.out.write("%s -> %s<br>" % (key, output))
    self.response.out.write("<body bgcolor='blue'></body>")

def main():
  application = webapp.WSGIApplication([('/', MainFrameHandler),
                                        ('/ready', ReadyHandler),
                                        ('/main', MainHandler),
                                        ('/runmap', RunMapHandler),
                                        ('/runreduce', RunReduceHandler),
                                        ('/map', MapHandler),
                                        ('/reduce', ReduceHandler)],
                                       debug=True)
  wsgiref.handlers.CGIHandler().run(application)


if __name__ == '__main__':
  main()
