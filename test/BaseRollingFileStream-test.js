"use strict";
var should = require('should')
, fs = require('fs');

describe('BaseRollingFileStream', function() {
  describe('when no filename is passed', function() {
    it('should throw an error', function() {
      var BaseRollingFileStream = require('../lib/BaseRollingFileStream');
      (function() {
        new BaseRollingFileStream();
      }).should.throw();
    });
  });

  describe('default behaviour', function() {
    var stream;

    before(function() {
      var BaseRollingFileStream = require('../lib/BaseRollingFileStream');
      stream = new BaseRollingFileStream('basetest.log');
    });

    after(function(done) {
      fs.unlink('basetest.log', done);
    });

    it('should not want to roll', function() {
      stream.shouldRoll().should.eql(false);
    });

    it('should not roll', function() {
      var cbCalled = false;
      //just calls the callback straight away, no async calls
      stream.roll('basetest.log', function() { cbCalled = true; });
      cbCalled.should.eql(true);
    });
  });
});
