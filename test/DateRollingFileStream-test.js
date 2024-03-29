require("should");

const fs = require("fs-extra"),
  path = require("path"),
  zlib = require("zlib"),
  proxyquire = require("proxyquire").noPreserveCache(),
  util = require("util"),
  streams = require("stream");

let fakeNow = new Date(2012, 8, 12, 10, 37, 11);
const mockNow = () => fakeNow;
const RollingFileWriteStream = proxyquire("../lib/RollingFileWriteStream", {
  "./now": mockNow
});
const DateRollingFileStream = proxyquire("../lib/DateRollingFileStream", {
  "./RollingFileWriteStream": RollingFileWriteStream
});

const gunzip = util.promisify(zlib.gunzip);
const gzip = util.promisify(zlib.gzip);
const remove = filename => fs.unlink(filename).catch(() => {});
const close = async (stream) => new Promise(
  (resolve, reject) => stream.end(e => e ? reject(e) : resolve())
);

describe("DateRollingFileStream", function() {
  describe("arguments", function() {
    let stream;

    before(function() {
      stream = new DateRollingFileStream(
        path.join(__dirname, "test-date-rolling-file-stream-1"),
        "yyyy-MM-dd.hh"
      );
    });

    after(async function() {
      await close(stream);
      await remove(path.join(__dirname, "test-date-rolling-file-stream-1"));
    });

    it("should take a filename and a pattern and return a WritableStream", function() {
      stream.filename.should.eql(
        path.join(__dirname, "test-date-rolling-file-stream-1")
      );
      stream.options.pattern.should.eql("yyyy-MM-dd.hh");
      stream.should.be.instanceOf(streams.Writable);
    });

    it("with default settings for the underlying stream", function() {
      stream.currentFileStream.mode.should.eql(0o600);
      stream.currentFileStream.flags.should.eql("a");
    });
  });

  describe("default arguments", function() {
    var stream;

    before(function() {
      stream = new DateRollingFileStream(
        path.join(__dirname, "test-date-rolling-file-stream-2")
      );
    });

    after(async function() {
      await close(stream);
      await remove(path.join(__dirname, "test-date-rolling-file-stream-2"));
    });

    it("should have pattern of yyyy-MM-dd", function() {
      stream.options.pattern.should.eql("yyyy-MM-dd");
    });
  });

  describe("with stream arguments", function() {
    var stream;

    before(function() {
      stream = new DateRollingFileStream(
        path.join(__dirname, "test-date-rolling-file-stream-3"),
        "yyyy-MM-dd",
        { mode: parseInt("0666", 8) }
      );
    });

    after(async function() {
      await close(stream);
      await remove(path.join(__dirname, "test-date-rolling-file-stream-3"));
    });

    it("should pass them to the underlying stream", function() {
      stream.theStream.mode.should.eql(parseInt("0666", 8));
    });
  });

  describe("with stream arguments but no pattern", function() {
    var stream;

    before(function() {
      stream = new DateRollingFileStream(
        path.join(__dirname, "test-date-rolling-file-stream-4"),
        { mode: parseInt("0666", 8) }
      );
    });

    after(async function() {
      await close(stream);
      await remove(path.join(__dirname, "test-date-rolling-file-stream-4"));
    });

    it("should pass them to the underlying stream", function() {
      stream.theStream.mode.should.eql(parseInt("0666", 8));
    });

    it("should use default pattern", function() {
      stream.options.pattern.should.eql("yyyy-MM-dd");
    });
  });

  describe("with a pattern of .yyyy-MM-dd", function() {
    var stream;

    before(function(done) {
      stream = new DateRollingFileStream(
        path.join(__dirname, "test-date-rolling-file-stream-5"),
        ".yyyy-MM-dd",
        null
      );
      stream.write("First message\n", "utf8", done);
    });

    after(async function() {
      await close(stream);
      await remove(path.join(__dirname, "test-date-rolling-file-stream-5"));
    });

    it("should create a file with the base name", async function() {
      const contents = await fs.readFile(
        path.join(__dirname, "test-date-rolling-file-stream-5"),
        "utf8"
      );
      contents.should.eql("First message\n");
    });

    describe("when the day changes", function() {
      before(function(done) {
        fakeNow = new Date(2012, 8, 13, 0, 10, 12);
        stream.write("Second message\n", "utf8", done);
      });

      after(async function() {
        await remove(
          path.join(__dirname, "test-date-rolling-file-stream-5..2012-09-12")
        );
      });

      describe("the number of files", function() {
        it("should be two", async function() {
          const files = await fs.readdir(__dirname);
          files
            .filter(
              file => file.indexOf("test-date-rolling-file-stream-5") > -1
            )
            .should.have.length(2);
        });
      });

      describe("the file without a date", function() {
        it("should contain the second message", async function() {
          const contents = await fs.readFile(
            path.join(__dirname, "test-date-rolling-file-stream-5"),
            "utf8"
          );
          contents.should.eql("Second message\n");
        });
      });

      describe("the file with the date", function() {
        it("should contain the first message", async function() {
          const contents = await fs.readFile(
            path.join(__dirname, "test-date-rolling-file-stream-5..2012-09-12"),
            "utf8"
          );
          contents.should.eql("First message\n");
        });
      });
    });
  });

  describe("with alwaysIncludePattern", function() {
    var stream;

    before(async function() {
      fakeNow = new Date(2012, 8, 12, 11, 10, 12);
      await remove(
        path.join(
          __dirname,
          "test-date-rolling-file-stream-pattern.2012-09-12-11.log"
        )
      );
      stream = new DateRollingFileStream(
        path.join(__dirname, "test-date-rolling-file-stream-pattern"),
        "yyyy-MM-dd-hh.log",
        { alwaysIncludePattern: true }
      );

      await new Promise(resolve => {
        setTimeout(function() {
          stream.write("First message\n", "utf8", () => resolve());
        }, 50);
      });
    });

    after(async function() {
      await close(stream);
      await remove(
        path.join(
          __dirname,
          "test-date-rolling-file-stream-pattern.2012-09-12-11.log"
        )
      );
    });

    it("should create a file with the pattern set", async function() {
      const contents = await fs.readFile(
        path.join(
          __dirname,
          "test-date-rolling-file-stream-pattern.2012-09-12-11.log"
        ),
        "utf8"
      );
      contents.should.eql("First message\n");
    });

    describe("when the day changes", function() {
      before(function(done) {
        fakeNow = new Date(2012, 8, 12, 12, 10, 12);
        stream.write("Second message\n", "utf8", done);
      });

      after(async function() {
        await remove(
          path.join(
            __dirname,
            "test-date-rolling-file-stream-pattern.2012-09-12-12.log"
          )
        );
      });

      describe("the number of files", function() {
        it("should be two", async function() {
          const files = await fs.readdir(__dirname);
          files
            .filter(
              file => file.indexOf("test-date-rolling-file-stream-pattern") > -1
            )
            .should.have.length(2);
        });
      });

      describe("the file with the later date", function() {
        it("should contain the second message", async function() {
          const contents = await fs.readFile(
            path.join(
              __dirname,
              "test-date-rolling-file-stream-pattern.2012-09-12-12.log"
            ),
            "utf8"
          );
          contents.should.eql("Second message\n");
        });
      });

      describe("the file with the date", function() {
        it("should contain the first message", async function() {
          const contents = await fs.readFile(
            path.join(
              __dirname,
              "test-date-rolling-file-stream-pattern.2012-09-12-11.log"
            ),
            "utf8"
          );
          contents.should.eql("First message\n");
        });
      });
    });
  });

  describe("with a pattern that evaluates to digits", function() {
    let stream;
    before(done => {
      fakeNow = new Date(2012, 8, 12, 0, 10, 12);
      stream = new DateRollingFileStream(
        path.join(__dirname, "digits.log"),
        "yyyyMMdd"
      );
      stream.write("First message\n", "utf8", done);
    });

    describe("when the day changes", function() {
      before(function(done) {
        fakeNow = new Date(2012, 8, 13, 0, 10, 12);
        stream.write("Second message\n", "utf8", done);
      });

      it("should be two files (it should not get confused by indexes)", async function() {
        const files = await fs.readdir(__dirname);
        var logFiles = files.filter(file => file.indexOf("digits.log") > -1);
        logFiles.should.have.length(2);

        const contents = await fs.readFile(
          path.join(__dirname, "digits.log.20120912"),
          "utf8"
        );
        contents.should.eql("First message\n");
        const c = await fs.readFile(path.join(__dirname, "digits.log"), "utf8");
        c.should.eql("Second message\n");
      });
    });

    after(async function() {
      await close(stream);
      await remove(path.join(__dirname, "digits.log"));
      await remove(path.join(__dirname, "digits.log.20120912"));
    });
  });

  describe("with compress option", function() {
    var stream;

    before(function(done) {
      fakeNow = new Date(2012, 8, 12, 0, 10, 12);
      stream = new DateRollingFileStream(
        path.join(__dirname, "compressed.log"),
        "yyyy-MM-dd",
        { compress: true }
      );
      stream.write("First message\n", "utf8", done);
    });

    describe("when the day changes", function() {
      before(function(done) {
        fakeNow = new Date(2012, 8, 13, 0, 10, 12);
        stream.write("Second message\n", "utf8", done);
      });

      it("should be two files, one compressed", async function() {
        const files = await fs.readdir(__dirname);
        var logFiles = files.filter(
          file => file.indexOf("compressed.log") > -1
        );
        logFiles.should.have.length(2);

        const gzipped = await fs.readFile(
          path.join(__dirname, "compressed.log.2012-09-12.gz")
        );
        const contents = await gunzip(gzipped);
        contents.toString("utf8").should.eql("First message\n");

        (await fs.readFile(
          path.join(__dirname, "compressed.log"),
          "utf8"
        )).should.eql("Second message\n");
      });
    });

    after(async function() {
      await close(stream);
      await remove(path.join(__dirname, "compressed.log"));
      await remove(path.join(__dirname, "compressed.log.2012-09-12.gz"));
    });
  });

  describe("with keepFileExt option", function() {
    var stream;

    before(function(done) {
      fakeNow = new Date(2012, 8, 12, 0, 10, 12);
      stream = new DateRollingFileStream(
        path.join(__dirname, "keepFileExt.log"),
        "yyyy-MM-dd",
        { keepFileExt: true }
      );
      stream.write("First message\n", "utf8", done);
    });

    describe("when the day changes", function() {
      before(function(done) {
        fakeNow = new Date(2012, 8, 13, 0, 10, 12);
        stream.write("Second message\n", "utf8", done);
      });

      it("should be two files", async function() {
        const files = await fs.readdir(__dirname);
        var logFiles = files.filter(file => file.indexOf("keepFileExt") > -1);
        logFiles.should.have.length(2);

        (await fs.readFile(
          path.join(__dirname, "keepFileExt.2012-09-12.log"),
          "utf8"
        )).should.eql("First message\n");
        (await fs.readFile(
          path.join(__dirname, "keepFileExt.log"),
          "utf8"
        )).should.eql("Second message\n");
      });
    });

    after(async function() {
      await close(stream);
      await remove(path.join(__dirname, "keepFileExt.log"));
      await remove(path.join(__dirname, "keepFileExt.2012-09-12.log"));
    });
  });

  describe("with compress option and keepFileExt option", function() {
    var stream;

    before(function(done) {
      fakeNow = new Date(2012, 8, 12, 0, 10, 12);
      stream = new DateRollingFileStream(
        path.join(__dirname, "compressedAndKeepExt.log"),
        "yyyy-MM-dd",
        { compress: true, keepFileExt: true }
      );
      stream.write("First message\n", "utf8", done);
    });

    describe("when the day changes", function() {
      before(function(done) {
        fakeNow = new Date(2012, 8, 13, 0, 10, 12);
        stream.write("Second message\n", "utf8", done);
      });

      it("should be two files, one compressed", async function() {
        const files = await fs.readdir(__dirname);
        var logFiles = files.filter(
          file => file.indexOf("compressedAndKeepExt") > -1
        );
        logFiles.should.have.length(2);

        const gzipped = await fs.readFile(
          path.join(__dirname, "compressedAndKeepExt.2012-09-12.log.gz")
        );
        const contents = await gunzip(gzipped);
        contents.toString("utf8").should.eql("First message\n");
        (await fs.readFile(
          path.join(__dirname, "compressedAndKeepExt.log"),
          "utf8"
        )).should.eql("Second message\n");
      });
    });

    after(async function() {
      await close(stream);
      await remove(path.join(__dirname, "compressedAndKeepExt.log"));
      await remove(
        path.join(__dirname, "compressedAndKeepExt.2012-09-12.log.gz")
      );
    });
  });

  describe("with fileNameSep option", function() {
    var stream;

    before(function(done) {
      fakeNow = new Date(2012, 8, 12, 0, 10, 12);
      stream = new DateRollingFileStream(
        path.join(__dirname, "fileNameSep.log"),
        "yyyy-MM-dd",
        { fileNameSep: "_" }
      );
      stream.write("First message\n", "utf8", done);
    });

    describe("when the day changes", function() {
      before(function(done) {
        fakeNow = new Date(2012, 8, 13, 0, 10, 12);
        stream.write("Second message\n", "utf8", done);
      });

      it("should be two files", async function() {
        const files = await fs.readdir(__dirname);
        var logFiles = files.filter(file => file.indexOf("fileNameSep") > -1);
        logFiles.should.have.length(2);

        (await fs.readFile(
          path.join(__dirname, "fileNameSep.log_2012-09-12"),
          "utf8"
        )).should.eql("First message\n");
        (await fs.readFile(
          path.join(__dirname, "fileNameSep.log"),
          "utf8"
        )).should.eql("Second message\n");
      });
    });

    after(async function() {
      await close(stream);
      await remove(path.join(__dirname, "fileNameSep.log"));
      await remove(path.join(__dirname, "fileNameSep.log_2012-09-12"));
    });
  });

  describe("with fileNameSep option and keepFileExt option", function() {
    var stream;

    before(function(done) {
      fakeNow = new Date(2012, 8, 12, 0, 10, 12);
      stream = new DateRollingFileStream(
        path.join(__dirname, "fileNameSepAndKeepExt.log"),
        "yyyy-MM-dd",
        { fileNameSep: "_", keepFileExt: true }
      );
      stream.write("First message\n", "utf8", done);
    });

    describe("when the day changes", function() {
      before(function(done) {
        fakeNow = new Date(2012, 8, 13, 0, 10, 12);
        stream.write("Second message\n", "utf8", done);
      });

      it("should be two files", async function() {
        const files = await fs.readdir(__dirname);
        var logFiles = files.filter(file => file.indexOf("fileNameSepAndKeepExt") > -1);
        logFiles.should.have.length(2);

        (await fs.readFile(
          path.join(__dirname, "fileNameSepAndKeepExt_2012-09-12.log"),
          "utf8"
        )).should.eql("First message\n");
        (await fs.readFile(
          path.join(__dirname, "fileNameSepAndKeepExt.log"),
          "utf8"
        )).should.eql("Second message\n");
      });
    });

    after(async function() {
      await close(stream);
      await remove(path.join(__dirname, "fileNameSepAndKeepExt.log"));
      await remove(path.join(__dirname, "fileNameSepAndKeepExt_2012-09-12.log"));
    });
  });

  describe("with fileNameSep option and compress option", function() {
    var stream;

    before(function(done) {
      fakeNow = new Date(2012, 8, 12, 0, 10, 12);
      stream = new DateRollingFileStream(
        path.join(__dirname, "fileNameSepAndCompressed.log"),
        "yyyy-MM-dd",
        { fileNameSep: "_", compress: true }
      );
      stream.write("First message\n", "utf8", done);
    });

    describe("when the day changes", function() {
      before(function(done) {
        fakeNow = new Date(2012, 8, 13, 0, 10, 12);
        stream.write("Second message\n", "utf8", done);
      });

      it("should be two files, one compressed", async function() {
        const files = await fs.readdir(__dirname);
        var logFiles = files.filter(
          file => file.indexOf("fileNameSepAndCompressed") > -1
        );
        logFiles.should.have.length(2);

        const gzipped = await fs.readFile(
          path.join(__dirname, "fileNameSepAndCompressed.log_2012-09-12.gz")
        );
        const contents = await gunzip(gzipped);
        contents.toString("utf8").should.eql("First message\n");
        (await fs.readFile(
          path.join(__dirname, "fileNameSepAndCompressed.log"),
          "utf8"
        )).should.eql("Second message\n");
      });
    });

    after(async function() {
      await close(stream);
      await remove(path.join(__dirname, "fileNameSepAndCompressed.log"));
      await remove(
        path.join(__dirname, "fileNameSepAndCompressed.log_2012-09-12.gz")
      );
    });
  });

  describe("with fileNameSep option, compress option and keepFileExt option", function() {
    var stream;

    before(function(done) {
      fakeNow = new Date(2012, 8, 12, 0, 10, 12);
      stream = new DateRollingFileStream(
        path.join(__dirname, "fileNameSepCompressedAndKeepExt.log"),
        "yyyy-MM-dd",
        { fileNameSep: "_", compress: true, keepFileExt: true }
      );
      stream.write("First message\n", "utf8", done);
    });

    describe("when the day changes", function() {
      before(function(done) {
        fakeNow = new Date(2012, 8, 13, 0, 10, 12);
        stream.write("Second message\n", "utf8", done);
      });

      it("should be two files, one compressed", async function() {
        const files = await fs.readdir(__dirname);
        var logFiles = files.filter(
          file => file.indexOf("fileNameSepCompressedAndKeepExt") > -1
        );
        logFiles.should.have.length(2);

        const gzipped = await fs.readFile(
          path.join(__dirname, "fileNameSepCompressedAndKeepExt_2012-09-12.log.gz")
        );
        const contents = await gunzip(gzipped);
        contents.toString("utf8").should.eql("First message\n");
        (await fs.readFile(
          path.join(__dirname, "fileNameSepCompressedAndKeepExt.log"),
          "utf8"
        )).should.eql("Second message\n");
      });
    });

    after(async function() {
      await close(stream);
      await remove(path.join(__dirname, "fileNameSepCompressedAndKeepExt.log"));
      await remove(
        path.join(__dirname, "fileNameSepCompressedAndKeepExt_2012-09-12.log.gz")
      );
    });
  });

  describe("using deprecated daysToKeep", () => {
    const onWarning = process.listeners("warning").shift();
    let wrapper;
    let stream;

    before(done => {
      const muteSelfDeprecation = (listener) => {
        return (warning) => {
          if (warning.name === "DeprecationWarning" && warning.code === "streamroller-DEP0001") {
            return;
          } else {
            listener(warning);
          }
        };
      };
      wrapper = muteSelfDeprecation(onWarning);
      process.prependListener("warning", wrapper);
      process.removeListener("warning", onWarning);
      done();
    });

    after(async () => {
      process.prependListener("warning", onWarning);
      process.removeListener("warning", wrapper);
      await close(stream);
      await remove(path.join(__dirname, "daysToKeep.log"));
    });

    it("should have deprecated warning for daysToKeep", () => {
      process.on("warning", (warning) => {
        warning.name.should.eql("DeprecationWarning");
        warning.code.should.eql("streamroller-DEP0001");
      });

      stream = new DateRollingFileStream(
        path.join(__dirname, "daysToKeep.log"),
        { daysToKeep: 4 }
      );
    });

    describe("with options.daysToKeep but not options.numBackups", () => {
      it("should be routed from options.daysToKeep to options.numBackups", () => {
        stream.options.numBackups.should.eql(stream.options.daysToKeep);
      });

      it("should be generated into stream.options.numToKeep from options.numBackups", () => {
        stream.options.numToKeep.should.eql(stream.options.numBackups + 1);
      });
    });

    describe("with both options.daysToKeep and options.numBackups", function() {
      let stream;
      it("should take options.numBackups to supercede options.daysToKeep", function() {
        stream = new DateRollingFileStream(
          path.join(__dirname, "numBackups.log"),
          {
            daysToKeep: 3,
            numBackups: 9
          }
        );
        stream.options.daysToKeep.should.not.eql(3);
        stream.options.daysToKeep.should.eql(9);
        stream.options.numBackups.should.eql(9);
        stream.options.numToKeep.should.eql(10);
      });

      after(async function() {
        await close(stream);
        await remove("numBackups.log");
      });
    });
  });

  describe("with invalid number of numBackups", () => {
    it("should complain about negative numBackups", () => {
      const numBackups = -1;
      (() => {
        new DateRollingFileStream(
          path.join(__dirname, "numBackups.log"),
          { numBackups: numBackups }
        );
      }).should.throw(`options.numBackups (${numBackups}) should be >= 0`);
    });

    it("should complain about numBackups >= Number.MAX_SAFE_INTEGER", () => {
      const numBackups = Number.MAX_SAFE_INTEGER;
      (() => {
        new DateRollingFileStream(
          path.join(__dirname, "numBackups.log"),
          { numBackups: numBackups }
        );
      }).should.throw(`options.numBackups (${numBackups}) should be < Number.MAX_SAFE_INTEGER`);
    });
  });

  describe("with numBackups option", function() {
    let stream;
    var numBackups = 4;
    var numOriginalLogs = 10;

    before(async function() {
      fakeNow = new Date(2012, 8, 13, 0, 10, 12); // pre-req to trigger a date-change later
      for (let i = 0; i < numOriginalLogs; i += 1) {
        await fs.writeFile(
          path.join(__dirname, `numBackups.log.2012-09-${20-i}`), 
          `Message on day ${i}\n`, 
          { encoding: "utf-8" }
        );
      }
      stream = new DateRollingFileStream(
        path.join(__dirname, "numBackups.log"),
        "yyyy-MM-dd",
        {
          alwaysIncludePattern: true,
          numBackups: numBackups
        }
      );
    });

    describe("when the day changes", function() {
      before(function(done) {
        fakeNow = new Date(2012, 8, 21, 0, 10, 12); // trigger a date-change
        stream.write("Test message\n", "utf8", done);
      });

      it("should be numBackups + 1 files left from numOriginalLogs", async function() {
        const files = await fs.readdir(__dirname);
        var logFiles = files.filter(
          file => file.indexOf("numBackups.log") > -1
        );
        logFiles.should.have.length(numBackups + 1);
      });
    });

    after(async function() {
      await close(stream);
      const files = await fs.readdir(__dirname);
      const logFiles = files
        .filter(file => file.indexOf("numBackups.log") > -1)
        .map(f => remove(path.join(__dirname, f)));
      await Promise.all(logFiles);
    });
  });

  describe("with numBackups and compress options", function() {
    let stream;
    const numBackups = 4;
    const numOriginalLogs = 10;

    before(async function() {
      fakeNow = new Date(2012, 8, 13, 0, 10, 12); // pre-req to trigger a date-change later
      for (let i = numOriginalLogs; i >= 0; i -= 1) {
        const contents = await gzip(`Message on day ${i}\n`);
        await fs.writeFile(
          path.join(__dirname, `compressedNumBackups.log.2012-09-${20-i}.gz`),
          contents
        );
      }
      stream = new DateRollingFileStream(
        path.join(__dirname, "compressedNumBackups.log"),
        "yyyy-MM-dd",
        {
          alwaysIncludePattern: true,
          compress: true,
          numBackups: numBackups
        }
      );
    });

    describe("when the day changes", function() {
      before(function(done) {
        fakeNow = new Date(2012, 8, 21, 0, 10, 12); // trigger a date-change
        stream.write("New file message\n", "utf8", done);
      });

      it("should be 5 files left from original 11", async function() {
        const files = await fs.readdir(__dirname);
        var logFiles = files.filter(
          file => file.indexOf("compressedNumBackups.log") > -1
        );
        logFiles.should.have.length(numBackups + 1);
      });
    });

    after(async function() {
      await close(stream);
      const files = await fs.readdir(__dirname);
      const logFiles = files
        .filter(file => file.indexOf("compressedNumBackups.log") > -1)
        .map(f => remove(path.join(__dirname, f)));
      await Promise.all(logFiles);
    });
  });
});
