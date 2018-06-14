require('dotenv').config();
const db = require("./models");
const Sequelize = require("sequelize");
const Op = Sequelize.Op;
const moment = require("moment");
const todayOnly = moment().format('YYYY-MM-DD');
const today = moment().format('YYYY-MM-DD 23:59:59');
const util = require("util");
const cTable = require('console.table');
const PORT = process.env.PORT || 8081;
const http = require("http");

var API_INTERVAL = process.env.API_INTERVAL;

var server = http.createServer(function (request, response) {
  response.writeHead(200, { "Content-Type": "text/html" });
  response.write("<!DOCTYPE \"html\">");
  response.write("<html>");
  response.write("<head>");
  response.write("<title>Hello World Page</title>");
  response.write("</head>");
  response.write("<body>");
  response.write("Hello World!");
  response.write("</body>");
  response.write("</html>");
  response.end();
});

function updateApiCounter() {
  const today = moment().utc().format('YYYY-MM-DD');
  const dayStart = moment().utc().startOf('day');
  let q1 = moment().utc().diff(dayStart, 's');
  const qPeriod = dayStart.add(q1 - q1 % 21600, 's').toISOString();
  let obj1 = {
    obj: db.ApiCounterQ,
    temporal: qPeriod,
    where: {
      qPeriod: qPeriod
    },
    limit: 250,
    handle: "6 Hoursly"
  };
  return new Promise((resolve, reject) => {
    obj1.obj.findOrCreate({
      where: obj1.where,
      defaults: {
        counter: 0,
        exceeded: false
      }
    })
      .spread((obj, created) => {
        if (obj.exceeded) {
          return reject("WARNING: " + obj1.handle + " call limit exceeded!");
        } else {
          obj.increment('counter', {
            by: 1
          })
            .then(obj => {
              if (obj.counter >= obj1.limit) {
                obj.update({ exceeded: true })
                  .then(() => {
                    return reject("WARNING: " + obj1.handle + " call limit exceeded!");
                  });
              } else {
                  let now = moment().utc();
                  let period = moment(obj.qPeriod ? obj.qPeriod : obj.date).utc();
                  let counter = parseInt(obj.counter);
                  console.log("NOW   : " + now)
                  console.log("PERIOD: " + period)
                  console.log("DIFF  : " + Math.floor(now.diff(period)))
                  console.log("Calls remaining: " + (250 - counter))
                  let newInterval = Math.floor(Math.floor(now.diff(period)) / (250 - counter));
                  newInterval = (newInterval >= 14000) ? newInterval : 14000
                  let newIntervalS = Math.floor(newInterval / 1000);
                  let intervalDiff = parseInt(API_INTERVAL) - newInterval;


                  if (newIntervalS >= 1) {
                    console.log("Old interval " + API_INTERVAL + " New interval: " + newIntervalS + " seconds between calls.  Need to " + ((intervalDiff > 0) ? "speed up" : "slow down") + " by " + Math.abs(intervalDiff) + " milliseconds.");
                    API_INTERVAL = newInterval
                    console.log("New interval " + API_INTERVAL);
                  }

                  let t = moment.duration(moment(moment()).utc().diff(obj.qPeriod));
                  resolve((created ? "New" : "Existing") + " API " + obj1.handle + " call counter for " + obj1.temporal + " is " + obj.counter + " with " + t + " seconds remaining in the " + obj.qPeriod + " period");
                
              }
            });
        }
      });
  });
}

function updateSourceNewestTime(dbSource, time) {
  dbSource.update({ newest: time })
  .then((dbSource) => {
      console.log("UPDATED NEWEST ARTICLE FOR "+dbSource.id+" TO " + time)
    })
    .catch(err => {
      console.log("ERROR updating source " + dbSource.id + ": " + err)
    })
}

function callApi(dbSource, startAt, pageNum, dbBacklog) {
  updateApiCounter()
    .then(() => {
      if (process.env.APISCHEDULER == "true") {
        const NewsAPI = require('newsapi');
        const newsapi = new NewsAPI(process.env.NEWSAPIKEY);
        newsapi.v2.everything({
          sources: dbSource.id,
          pageSize: 100,
          page: pageNum,
          from: startAt,
          sortBy: 'publishedAt'
        }).then(response => {
          console.log("CALLAPI startAt: " + startAt)
          if (response.status == "ok" && response.totalResults) {
            var totalPages = Math.floor(response.totalResults / 100)
            console.log("SOURCE: " + dbSource.id + " PAGE: " + pageNum + " TOTAL RESULTS: " + response.totalResults + " TOTAL PAGES REMAINING: " + totalPages);
            if (pageNum == 1 && totalPages > 0) {
              db.Backlog.create({
                source: dbSource.id,
                date: startAt,
                totalArticles: response.totalResults,
                totalPages: totalPages,
                pagesRetrieved: 1,
                startAt: startAt
              })
              .catch(err => {
                console.log("ERROR: Creating Backlog " + err)
              })
            } else if (dbBacklog) {
              dbBacklog.increment('pagesRetrieved', {
                by: 1
              })
              .then(() => {
                console.log("PAGES RETRIEVED: " + dbBacklog.pagesRetrieved + "/" + totalPages);
                if (dbBacklog.pagesRetrieved >= dbBacklog.totalPages) {
                  console.log("Removing Backlog for " + db.Backlog[dbSource.id])
                  dbBacklog.destroy();
                  if (typeof response.articles[0] != 'undefined') {
                    updateSourceNewestTime(dbSource, response.articles[0].publishedAt)
                  }
                }
                });
            }
            if (typeof response.articles[0] != 'undefined') {
              let newStartAt = moment(startAt).toISOString(false);
              startAt = moment(startAt).toISOString(false);
              (response.articles).forEach(article => {
                article.SourceId = article.source.id;
                article.source = article.source.id;
                article.publishedAt = moment(article.publishedAt).toISOString(false);
                console.log("ADDED: " + article.publishedAt, article.title)
                console.log("PUBLISHED: "+ article.publishedAt);
                console.log("SA       : "+ startAt);
                console.log("NSA      : "+ newStartAt);
            db.Article.create(article)
                  .then(() => {
                    // console.log("TEST " + article.publishedAt + " " + newStartAt + " " + article.title)
                    if (moment(article.publishedAt).isSameOrAfter(moment(startAt)) && moment(article.publishedAt).isSameOrAfter(newStartAt)) {
                      newStartAt = moment(article.publishedAt).toISOString(false);
                      updateSourceNewestTime(source, newStartAt)
                    }
                  })
                  .catch(error => {
                    // These are validation errors because the row already exists.
                    // console.log("ERROR: "+error+" " + article.publishedAt, article.title)
                  })
              });
            } else {
              console.log("Response articles is UNDEFINED")
            }
          } else {
            console.log("BAD response to api call ==> " + JSON.stringify(response, null, 2));
          }
        })
        .catch((err) => {
          console.log("BAD response to api call ==> " + JSON.stringify(err, null, 2));
          // If this was a 
          //    maximumResultsReached: You have requested too many results. Developer accounts are limited to a max of 100 results. Please upgrade to a paid plan if you need more results.
          // then change newest to one day ago.
          if (JSON.stringify(err).match('maximumResultsReached')) {
            // Unfortunately it seems that minus one day is to much.  resetting this to 4 hours ago.
            newStartAt = moment().subtract(4,'hours')
            console.log(`Original startAt: ${moment(startAt).format("YYYY-MM-DD HH:mm:SS")}  New startAt: ${newStartAt}`)
            dbSource.update({ newest: newStartAt})
          }
        })
      }
    })
    .catch(err => {
      console.log(err)
      // clearInterval(intervalObj)
    })
}

if (process.env.APISCHEDULER == "true") {
  console.log("Api scheduler will run at " + API_INTERVAL + " ms intervals")
}

function procSources(dbSources) {
  const now = moment().utc()
  let dbSource = dbSources[sourceIdx]
  let startAt = moment(dbSource.newest).toISOString(false)
  console.log("########################################################################################################")
  console.log("Querying Source " + dbSource.id + " " + sourceIdx + "/" + dbSources.length + " TODAY: " + now.format('YYYY-MM-DD') + " HOUR: " + now.format('YYYY-MM-DD HH:00:00') + " starting at  " + startAt)
  console.table(dbSource.dataValues);
  db.Backlog.findOne({
    where: {
      source: dbSource.id
    }
  })
    .then(dbBacklog => {
      callApi(dbSource, dbBacklog.startAt, dbBacklog.pagesRetrieved + 1, dbBacklog);
    })
    .catch(err => {
      console.log('Error occured reading backlog -> '+JSON.stringify(err, null, 2));
      callApi(dbSource, startAt, 1);
    })
    .then(() => {
      let apiSchedulerInterval = setTimeout(function () {
        if (sourceIdx < dbSources.length - 1) {
          sourceIdx++;
          console.log("Go to next source");
        } else {
          sourceIdx = 0;
          console.log(dbSource.id + "=================================== Done with all sources");
        }
        console.log(`Completed processing source ${dbSource.id}.  Interval is ${API_INTERVAL}.`)
        procSources(dbSources)
      }, API_INTERVAL)

    })
};

function sourceLoop() {
  console.log("#################### Starting loop through sources by oldest updated.")
  try {
    db.Source.findAll({
      order: [
        ['newest', 'ASC']
      ]
    }).then(dbSources => { procSources(dbSources) });
  } catch (err) {
    throw err;
  }
}

db.sequelize.sync().then(function () {
  server.listen(PORT);
  console.log("Server is listening");
})

let sourceIdx = 0;
sourceLoop();

// speedup_functionality