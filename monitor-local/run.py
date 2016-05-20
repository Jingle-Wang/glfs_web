from web import app,query_periodically
import thread

if __name__ == '__main__':
    thread.start_new_thread(query_periodically, ())
    app.run('0.0.0.0')
