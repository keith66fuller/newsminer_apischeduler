module.exports = function (sequelize, DataTypes) {
  var Backlog = sequelize.define("Backlog", {
    source: {
      type: DataTypes.STRING
    },
    date: DataTypes.DATEONLY,
    totalArticles: {
      type: DataTypes.INTEGER
    },
    totalPages: {
      type: DataTypes.INTEGER
    },
    pagesRetrieved: {
      type: DataTypes.INTEGER
    },
    startAt: DataTypes.DATE
  }, {
    timestamps: true,
    indexes: [ { unique: true, fields: [ 'source', 'date' ] } ]
  });

  return Backlog;
};