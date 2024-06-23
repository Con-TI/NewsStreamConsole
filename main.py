from flask import Flask, render_template
import fetch_news
import time

app = Flask(__name__)
fetch_news.main()

@app.route('/')
def home():
    df = fetch_news.main2()
    t = time.asctime(time.localtime())
    return render_template('index.html',time=t,table = df.to_html())

if __name__=='__main__':
    app.run(debug=False)