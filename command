python3 -m venv venv
. venv/bin/activate


export FLASK_APP=ServiceSniffer
export FLASK_ENV=development
flask run


flask init-db