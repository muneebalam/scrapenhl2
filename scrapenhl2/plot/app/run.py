"""
This file runs the app.
"""
import webbrowser

from scrapenhl2.plot import app


def runapp(debug=False):
    webbrowser.open('http://127.0.0.1:5000/', new=2)
    app.app.run()


if __name__ == '__main__':
    runapp(debug=True)
