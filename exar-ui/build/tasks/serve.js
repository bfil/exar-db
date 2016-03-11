var gulp = require('gulp');
var browserSync = require('browser-sync');
var historyApiFallback = require('connect-history-api-fallback')

function enableCORS(req, res, next) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');
  res.setHeader('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Connection, Content-Type, Accept, Accept-Encoding, Accept-Language,' +
    ' Host, Referer, User-Agent, Overwrite, Destination, Depth, X-Token, X-File-Size, If-Modified-Since, X-File-Name, Cache-Control');
  if (req.method.match(/^OPTIONS$/i)) return res.end();
  return next();
}

gulp.task('serve', ['build'], function(done) {
  browserSync({
    open: false,
    port: 8000,
    server: {
      baseDir: ['.'],
      middleware: [ historyApiFallback(), enableCORS ]
    }
  }, done);
});