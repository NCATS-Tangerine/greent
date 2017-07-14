from flask import Flask
from flask_graphql import GraphQLView
from .schema import Schema

def create_app(path='/graphql', **kwargs):
    app = Flask(__name__)
    app.debug = True
    app.add_url_rule(path, view_func=GraphQLView.as_view('graphql', schema=Schema, **kwargs))
    return app

def main ():
    app = create_app(graphiql=True)
    app.run()

if __name__ == '__main__':
    main ()
