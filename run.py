#!flask/bin/python

from app import app,query_periodically
import thread

thread.start_new_thread(query_periodically, ())
app.run(debug=True)
