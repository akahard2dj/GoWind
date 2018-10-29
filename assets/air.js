(require(["when", "jquery", "d3", "topojson", "mvi"], function() {
    "use strict";

    var tau = 2 * Math.PI;
    var MAX_TASK_TIME = 100;  // amount of time before a task yields control (milliseconds)
    var MIN_SLEEP_TIME = 25;  // amount of time a task waits before resuming (milliseconds)
    var INVISIBLE = -1;  // an invisible vector
    var NIL = -2;       // non-existent vector
    var RESOLUTION = 2;


    var MAP_SVG_ID = '#map-svg';
    var DISPLAY_ID = "#display";
    var LOCATION_ID = "#location"
    var STATUS_ID = "#status";
    var POINT_DETAILS_ID = "#point-details";
    var FIELD_CANVAS_ID = "#field-canvas";
    var OVERLAY_CANVAS_ID = "#overlay-canvas";

    // korea atmosphere environmental standard
    // http://www.me.go.kr/mamo/web/index.do?menuId=586
    // one hour averaged
    var OVERLAY_TYPES = {
        "temperature": {min: -20,   max: 60,    scale: "line", precision: 1, label: "기온 Temperature", unit: "ºC"},
        "humidity":  {min: 0,     max: 100,   scale: "line", precision: 1, label: "습도 Humidity", unit: "%"},
        "no2":  {min: 0.001, max: 0.1, scale: "log",  precision: 0, label: "이산화질소 Nitrogen Dioxide", unit: " ppb", multiplier: 1000},
        "o3":   {min: 0.001,   max: 0.1,   scale: "log",  precision: 1, label: "오존 Ozone", unit: " ppm"},
        "so2":  {min: 0.001, max: 0.15, scale: "log",  precision: 0, label: "이산화황 Sulfur Dioxide", unit: " ppb", multiplier: 1000},
        "co":   {min: 0.1,   max: 25,   scale: "log",  precision: 1, label: "일산화탄소 Carbon Monoxide", unit: " ppm"},
        "pm25": {min: 1,     max: 50,   scale: "log",  precision: 0, label: "미세먼지 2.5µm Particulate Matter", unit: ' μg/m<span class="sup">3</span>'},
        "pm10": {min: 1,     max: 100,   scale: "log",  precision: 0, label: "미세먼지 10µm Particulate Matter", unit: ' μg/m<span class="sup">3</span>'}
    };


    function formatOverlayValue(v) {
        v = Math.min(v, overlayType.max);
        v = Math.max(v, Math.min(overlayType.min, 0));
        if (overlayType.multiplier) {
            v *= overlayType.multiplier;
        }
        return v.toFixed(overlayType.precision) + overlayType.unit;
    }

    var displayData = {
        topography: d3.select(DISPLAY_ID).attr("data-topography"),
        samples: d3.select(DISPLAY_ID).attr("data-samples"),
        type: d3.select(DISPLAY_ID).attr("data-type")
    };
    var overlayType = OVERLAY_TYPES[displayData.type];

    function init() {
        if ("ontouchstart" in document.documentElement) {
            document.addEventListener("touchstart", function() {}, false);  // this hack enables :active pseudoclass
        }
        else {
            document.documentElement.className += " no-touch";  // to filter styles problematic for touch
        }

        // Modify the display elements to fill the screen.
        d3.select(MAP_SVG_ID).attr("width", view.width).attr("height", view.height);
        d3.select(FIELD_CANVAS_ID).attr("width", view.width).attr("height", view.height);
        d3.select(OVERLAY_CANVAS_ID).attr("width", view.width).attr("height", view.height);

        function addNavToSampleType(type) {
            d3.select("#" + type).on("click", function() {
                document.getElementById("display").setAttribute("data-type", type);
                document.getElementById("display").setAttribute("data-samples", "/data/current/");
                window.location.replace("/map/current/" + type);
            });
        }
        for (var type in OVERLAY_TYPES) {
            if (OVERLAY_TYPES.hasOwnProperty(type)) {
                addNavToSampleType(type);
            }
        }
        addNavToSampleType("wind");  // add the "None" overlay
    }

    function createSettings(topo) {
        var isFF = /firefox/i.test(navigator.userAgent);
        var bbox = [ 126.764387, 37.402429, 127.192733, 37.702704];
        var projection = createMercatorProjection(bbox[0], bbox[1], bbox[2], bbox[3], view);
        var bounds = createDisplayBounds(bbox[0], bbox[1], bbox[2], bbox[3], projection);
        var styles = [];
        var settings = {
            projection: projection,
            displayBounds: bounds,
            particleCount: Math.abs(Math.round(bounds.height)),
            maxParticleAge: 40,
            velocityScale: +(bounds.height / 1000).toFixed(3),
            fieldMaskWidth: isFF ? 1 :Math.ceil(bounds.height * 0.06),
            fadeFillStyle: isFF ? "rgba(0,0,0,0.95)" : "rgba(0, 0, 0, 0.97)",
            frameRate: 40,
            animate: true,
            styles: styles,
            styleIndex: function(m) {
                return Math.floor(Math.min(m, 10) / 10 * (styles.length - 1));
            }
        };
        log.debug(JSON.stringify(view) + " " + JSON.stringify(settings));
        for (var j = 85; j<= 255; j+= 5) {
            styles.push(asColorStyle(j, j, j, 0.6));
        }
        return settings;
    }

    function createMercatorProjection(lng0, lat0, lng1, lat1, view) {
        var projection = d3.geo.mercator()
            .center([126.9895, 37.5651])
            .scale(1)
            .translate([0,0]);
        var p0 = projection([lng0, lat0]);
        var p1 = projection([lng1, lat1]);

        var s = 1 / Math.max((p1[0] - p0[0]) / view.width, (p0[1]-p1[1])/view.height)*0.96;
        var t = [view.width/2, view.height/2];

        return projection.scale(s).translate(t);
    }

    function createDisplayBounds(lng0, lat0, lng1, lat1, projection) {
        var upperLeft = projection([lng0, lat1]).map(Math.floor);
        var lowerRight = projection([lng1, lat0]).map(Math.ceil);
        return {
            x: upperLeft[0],
            y: upperLeft[1],
            width: lowerRight[0] - upperLeft[0] + 1,
            height: lowerRight[1] - upperLeft[1] + 1
        }
    }

    function buildMeshes(topo, settings) {
        //displayStatus("building meshes...");
        log.time("building meshes");
        var path = d3.geo.path().projection(settings.projection);
        var outerBoundary = topojson.feature(topo, topo.objects.seoul_municipalities_geo, function(a, b) { return a === b; });
        var divisionBoundaries = topojson.feature(topo, topo.objects.seoul_municipalities_geo, function (a, b) { return a !== b; });
        log.timeEnd("building meshes");
        return {
            path: path,
            outerBoundary: outerBoundary,
            divisionBoundaries: divisionBoundaries
        };
    }

    var view = function() {
        var w = window;
        var d = document.documentElement;
        var b = document.getElementsByTagName("body")[0];
        var x = w.innerWidth || d.clientWidth || b.clientWidth;
        var y = w.innerHeight || d.clientHeight || b.clientHeight;
        return {width: x, height: y};
    }();

    function renderMasks(mesh, settings) {
        displayStatus("Rendering masks...");
        log.time("render masks");

        // To build the masks, re-render the map to a detached canvas and use the resulting pixel data array.
        // The red color channel defines the field mask, and the green color channel defines the display mask.

        var canvas = document.createElement("canvas");  // create detached canvas
        d3.select(canvas).attr("width", view.width).attr("height", view.height);
        var g = canvas.getContext("2d");
        var path = d3.geo.path().projection(settings.projection).context(g);  // create a path for the canvas

        path(mesh.outerBoundary);  // define the border

        // draw a fat border in red
        g.strokeStyle = asColorStyle(0, 0, 0, 1);
        g.lineWidth = settings.fieldMaskWidth;
        g.stroke();

        // fill the interior with both red and green
        g.fillStyle = asColorStyle(255, 255, 0, 1);
        g.fill();

        // draw a small border in red, slightly shrinking the display mask so we don't draw particles directly
        // on top of the visible SVG border
        g.strokeStyle = asColorStyle(255, 255, 0, 1);
        g.lineWidth = 2;
        g.stroke();

        // d3.select(DISPLAY_ID).node().appendChild(canvas);  // uncomment to make mask visible for debugging

        var width = canvas.width;
        var data = g.getImageData(0, 0, canvas.width, canvas.height).data;

        log.timeEnd("render masks");

        // data array layout: [r, g, b, a, r, g, b, a, ...]
        return {
            fieldMask: function(x, y) {
                var i = (y * width + x) * 4;  // red channel is field mask
                return data[i] > 0;
            },
            displayMask: function(x, y) {
                var i = (y * width + x) * 4 + 1;  // green channel is display mask
                return data[i] > 0;
            }
        }
    }

    function nap(value) {
        var d = when.defer();
        setTimeout(function() { d.resolve(value); }, MIN_SLEEP_TIME);
        return d.promise;
    }


    function render(settings, mesh) {
        return when(renderMap(mesh))
            .then(nap)
            .then(renderMasks.bind(null, mesh, settings));
    }

    function renderMap(mesh) {
        //displayStatus("building meshess...");
        log.time("building meshes");
        var mapSvg = d3.select(MAP_SVG_ID);
        mapSvg.selectAll("path").data(mesh.outerBoundary.features).enter().append("path").attr("class", "out-boundary").attr("d", mesh.path);
        mapSvg.selectAll("path").data(mesh.divisionBoundaries.features).enter().append("paht").attr("class", "in-boundary").attr("d", mesh.path);
        log.timeEnd("rendering map");
    }

    function plotStations(data, mesh) {
        var features = [];
        data.forEach(function(e) {
            if (isValidSample(e.wind)) {
                features.push({
                    type: "Features",
                    properties: {name: e.name},
                    geometry: {type: "Point", coordinates: e.coordinates}
                });
            }
        });
        mesh.path.pointRadius(1.5);
            d3.select(MAP_SVG_ID).append("path")
                .datum({type: "FeatureCollection", features: features})
                .attr("class", "station")
                .attr("d", mesh.path)
                .on("mouseover", function(d) {
                    div.transition().duration(200).style("opacity", .9);
                });
    }

    function drawOverlay(data, settings, masks) {
        if (!overlayType) {
            return when.resolve(null);
        }

        log.time("drawing overlay");
        var d = when.defer();

        if (data.length === 0) {
            return d.reject("No Data in Response");
        }

        var points = buildPointsFromSamples(data, settings.projection, function(sample) {
            var datum = sample[displayData.type];
            return datum == +datum ? datum : null;
        });

        if (points.length < 3) {  // we need at least three samples to interpolate
            return d.reject("현재 서울시 기상 정보가 부족합니다.");
        }

        var min = overlayType.min;
        var max = overlayType.max;
        var range = max - min;
        var rigidity = range * 0.05;  // use 5% of range as the rigidity

        var interpolate = mvi.thinPlateSpline(points, rigidity);

        var g = d3.select(OVERLAY_CANVAS_ID).node().getContext("2d");
        var isLogarithmic = (overlayType.scale === "log");
        var LN101 = Math.log(101);
        var bounds = settings.displayBounds;
        var displayMask = masks.displayMask;
        var xBound = bounds.x + bounds.width;  // upper bound (exclusive)
        var yBound = bounds.y + bounds.height;  // upper bound (exclusive)
        var x = bounds.x;

        // Draw color scale for reference.
        var n = view.width / 5;
        for (var i = n; i >= 0; i--) {
            g.fillStyle = asRainbowColorStyle((1 - (i / n)), 0.9);
            g.fillRect(view.width - 20 - i, view.height - 20, 1, 10);
        }

        // Draw a column by interpolating a value for each point and painting a 2x2 rectangle
        function drawColumn(x) {
            for (var y = bounds.y; y < yBound; y += RESOLUTION) {
                if (displayMask(x, y)) {
                    // Clamp interpolated z value to the range [min, max].
                    var z = Math.min(Math.max(interpolate(x, y), min), max);
                    // Now map to range [0, 1].
                    z = (z - min) / range;
                    if (isLogarithmic) {
                        // Map to logarithmic range [1, 101] then back to [0, 1]. Seems legit.
                        z = Math.log(z * 100 + 1) / LN101;
                    }
                    g.fillStyle = asRainbowColorStyle(z, 0.3);
                    g.fillRect(x, y, RESOLUTION, RESOLUTION);
                }
            }
        }

        (function batchDraw() {
            try {
                var start = +new Date;
                while (x < xBound) {
                    drawColumn(x);
                    x += RESOLUTION;
                    if ((+new Date - start) > MAX_TASK_TIME) {
                        // Drawing is taking too long. Schedule the next batch for later and yield.
                        setTimeout(batchDraw, MIN_SLEEP_TIME);
                        return;
                    }
                }
                d.resolve(interpolate);
                log.timeEnd("drawing overlay");
            }
            catch (e) {
                d.reject(e);
            }
        })();

        return d.promise;
    }


    function isValidSample(wind) {
        return wind[0] == +wind[0] && wind[1] == +wind[1];
    }

    function componentize(wind) {
        var phi = wind[0] / 360 * tau;
        var m = wind[1]; // wind velocity, m/s
        var u = -m * Math.sin(phi); // u comp., zonal velocity
        var v = -m * Math.cos(phi); // v comp., meridional velocity
        return [u, -v]; // neg v because pixel space grows downwards
    }

    function formatCoordinates(lng, lat) {
        return Math.abs(lat).toFixed(6) + "º " + (lat >= 0 ? "N" : "S") + ", " +
            Math.abs(lng).toFixed(6) + "º " + (lng >= 0 ? "E" : "W");
    }

    function formatVector(x, y) {
        var d = Math.atan2(-x, y) / τ * 360;  // calculate into-the-wind cardinal degrees
        var wd = Math.round((d + 360) % 360 / 5) * 5;  // shift [-180, 180] to [0, 360], and round to nearest 5.
        var m = Math.sqrt(x * x + y * y);
        return wd.toFixed(0) + "º @ " + m.toFixed(1) + " m/s";
    }

    function asRainbowColorStyle(hue, a) {
        // Map hue [0, 1] to radians [0, 5/6τ]. Don't allow a full rotation because that keeps hue == 0 and
        // hue == 1 from mapping to the same color.
        var rad = hue * tau * 5/6;
        rad *= 0.75;  // increase frequency to 2/3 cycle per rad

        var s = Math.sin(rad);
        var c = Math.cos(rad);
        var r = Math.floor(Math.max(0, -c) * 255);
        var g = Math.floor(Math.max(s, 0) * 255);
        var b = Math.floor(Math.max(c, 0, -s) * 255);
        return asColorStyle(r, g, b, a);
    }

    function animate(settings, field) {
        var bounds = settings.displayBounds;
        var buckets = settings.styles.map(function() { return []; });
        var particles = [];
        for (var i = 0; i < settings.particleCount; i++) {
            particles.push(field.randomize({age: rand(0, settings.maxParticleAge)}));
        }
        function evolve() {
            buckets.forEach(function(bucket) { bucket.length = 0; });
            particles.forEach(function(particle) {
                if (particle.age > settings.maxParticleAge) {
                    field.randomize(particle).age = 0;
                }
                var x = particle.x;
                var y = particle.y;
                var v = field(x, y);  // vector at current position
                var m = v[2];
                if (m === NIL) {
                    particle.age = settings.maxParticleAge;  // particle has escaped the grid, never to return...
                }
                else {
                    var xt = x + v[0];
                    var yt = y + v[1];
                    if (m > INVISIBLE && field(xt, yt)[2] > INVISIBLE) {
                        // Path from (x,y) to (xt,yt) is visible, so add this particle to the appropriate draw bucket.
                        particle.xt = xt;
                        particle.yt = yt;
                        buckets[settings.styleIndex(m)].push(particle);
                    }
                    else {
                        // Particle isn't visible, but it still moves through the field.
                        particle.x = xt;
                        particle.y = yt;
                    }
                }
                particle.age += 1;
            });
        }

        var g = d3.select(FIELD_CANVAS_ID).node().getContext("2d");
        g.lineWidth = 1.0;
        g.fillStyle = settings.fadeFillStyle;

        function draw() {
            // Fade existing particle trails.
            var prev = g.globalCompositeOperation;
            g.globalCompositeOperation = "destination-in";
            g.fillRect(bounds.x, bounds.y, bounds.width, bounds.height);
            g.globalCompositeOperation = prev;

            // Draw new particle trails.
            buckets.forEach(function(bucket, i) {
                if (bucket.length > 0) {
                    g.beginPath();
                    g.strokeStyle = settings.styles[i];
                    bucket.forEach(function(particle) {
                        g.moveTo(particle.x, particle.y);
                        g.lineTo(particle.xt, particle.yt);
                        particle.x = particle.xt;
                        particle.y = particle.yt;
                    });
                    g.stroke();
                }
            });
        }

        (function frame() {
            try {
                if (settings.animate) {
                    // var start = +new Date;
                    evolve();
                    draw();
                    // var duration = (+new Date - start);
                    setTimeout(frame, settings.frameRate /* - duration*/);
                }
            }
            catch (e) {
                report(e);
            }
        })();
    }

    function createField(columns) {
        var nilVector = [NaN, NaN, NIL];
        var field = function(x, y) {
            var column = columns[Math.round(x)];
            if (column) {
                var v = column[Math.round(y) - column[0]];  // the 0th element is the offset--see interpolateColumn
                if (v) {
                    return v;
                }
            }
            return nilVector;
        };

        // Create a function that will set a particle to a random location in the field. To do this uniformly and
        // efficiently given the field's sparse data structure, we build a running sum of column widths, starting at 0:
        //     [0, 10, 25, 29, ..., 100]
        // Each value represents the index of the first point in that column, and the last element is the total
        // number of points. Choosing a random point means generating a random number between [0, total), then
        // finding the column that contains this point by doing a binary search on the array. For example, point #27
        // corresponds to w[2] and therefore columns[2]. If columns[2] has the form [1041, a, b, c, d], then point
        // #27's coordinates are {x: 2, y: 1043}, where 1043 == 27 - 25 + 1 + 1041, and the value at that point is 'c'.

        field.randomize = function() {
            var w = [0];
            for (var i = 1; i <= columns.length; i++) {
                var column = columns[i - 1];
                w[i] = w[i - 1] + (column ? column.length - 1 : 0);
            }
            var pointCount = w[w.length - 1];

            return function(o) {
                var p = Math.floor(rand(0, pointCount));  // choose random point index
                var x = binarySearch(w, p);  // find column that contains this point
                x = x < 0 ? -x - 2 : x;  // when negative, x refers to _following_ column, so flip and go back one
                while (!columns[o.x = x]) {  // skip columns that have no points
                    x++;
                }
                // use remainder of point index to index into column, then add the column's offset to get actual y
                o.y = p - w[x] + 1 + columns[x][0];
                return o;
            }
        }();
        return field;
    }

    function binarySearch(a, v) {
        var low = 0, high = a.length - 1;
        while (low <= high) {
            var mid = low + ((high - low) >> 1), p = a[mid];
            if (p < v) {
                low = mid + 1;
            }
            else if (p === v) {
                return mid;
            }
            else {
                high = mid - 1;
            }
        }
        return -(low + 1);
    }


    function rand(min, max) {
        return min + Math.random() * (max - min);
    }

    function interpolateField(data, settings, masks) {
        log.time("interpolating field");
        var d = when.defer();

        if (data.length === 0) {
            return d.reject("No Data in Response");
        }

        var points = buildPointsFromSamples(data, settings.projection, function(sample) {
            return isValidSample(sample.wind) ? componentize(sample.wind) : null;
        });

        var interpolate = mvi.inverseDistanceWeighting(points, 5);

        var columns = [];
        var bounds = settings.displayBounds;
        var displayMask = masks.displayMask;
        var fieldMask = masks.fieldMask;
        var xBound = bounds.x + bounds.width;
        var yBound = bounds.y + bounds.height;
        var x = bounds.x;

        function interpolateColumn(x) {
            // Find min and max y coordinates in the column where the field mask is defined.
            var yMin, yMax;
            for (yMin = 0; yMin < yBound && !fieldMask(x, yMin); yMin++) {
            }
            for (yMax = yBound - 1; yMax > yMin && !fieldMask(x, yMax); yMax--) {
            }

            if (yMin <= yMax) {
                // Interpolate a vector for each valid y in the column. A column may have a long empty region at
                // the front. To save space, eliminate this empty region by encoding an offset in the column's 0th
                // element. A column with only three points defined at y=92, 93 and 94, would have an offset of 91
                // and a length of four. The point at y=92 would be column[92 - column[0]] === column[1].

                var column = [];
                var offset = column[0] = yMin - 1;
                for (var y = yMin; y <= yMax; y++) {
                    var v = null;
                    if (fieldMask(x, y)) {
                        v = [0, 0, 0];
                        v = interpolate(x, y, v);
                        v[2] = displayMask(x, y) ? Math.sqrt(v[0] * v[0] + v[1] * v[1]) : INVISIBLE;
                        v = mvi.scaleVector(v, settings.velocityScale);
                    }
                    column[y - offset] = v;
                }
                return column;
            }
            else {
                return null;
            }
        }

        (function batchInterpolate() {
            try {
                var start = +new Date;
                while (x < xBound) {
                    columns[x] = interpolateColumn(x);
                    x += 1;
                    if ((+new Date - start) > MAX_TASK_TIME) {
                        // Interpolation is taking too long. Schedule the next batch for later and yield.
                        displayStatus("Interpolating: " + x + "/" + xBound);
                        setTimeout(batchInterpolate, MIN_SLEEP_TIME);
                        return;
                    }
                }
                //var date = data[0].date.replace(":00+09:00", "");
                //d3.select(DISPLAY_ID).attr("data-date", displayData.date = date);
                //displayStatus(date + " JST");
                d.resolve(createField(columns));
                log.timeEnd("interpolating field");
            }
            catch (e) {
                d.reject(e);
            }
        })();

        return d.promise;

    }

    function buildPointsFromSamples(samples, projection, transform) {
        var points = [];
        samples.forEach(function(sample) {
            var point = projection(sample.coordinates);
            var value = transform(sample);
            if (value !== null) {
                points.push([point[0], point[1], value]);
            }
        });
        return points;
    }

    var bad = false;
    function displayStatus(status, error) {
        if (error) {
            bad = true;  // errors are sticky--let's not overwrite error information if it occurs
            //d3.select(STATUS_ID).node().textContent = "⁂ " + error;
        }
        else if (!bad) {
            //d3.select(STATUS_ID).node().textContent = "⁂ " + status;
        }
    }

    function apply(f) {
        return function(args) {
            return f.apply(null, args);
        }
    }

    var log = {
        debug:   function(s) { if (console && console.log) console.log(s); },
        info:    function(s) { if (console && console.info) console.info(s); },
        error:   function(e) { if (console && console.error) console.error(e.stack ? e + "\n" + e.stack : e); },
        time:    function(s) { if (console && console.time) console.time(s); },
        timeEnd: function(s) { if (console && console.timeEnd) console.timeEnd(s); }
    };

    function asColorStyle(r, g, b, a) {
        return "rgba(" + r + ", " + g + ", " + b + ", " + a + ")";
    }

    function loadJson(resource) {
        var d = when.defer();
        d3.json(resource, function(error, result) {
            return error ?
                !error.status ?
                    d.reject({error: -1, message: "Cannot load resource: " + resource, resource: resource}) :
                    d.reject({error: error.status, message: error.statusText, resource: resource}) :
                d.resolve(result);
        });
        return d.promise;
    }

    function postInit(settings, field, overlay) {
        d3.select(DISPLAY_ID).on("click", function() {
            var p = d3.mouse(this);
            var c = settings.projection.invert(p);
            var v = field(p[0], p[1]);
            if (v[2] >= INVISIBLE) {
                d3.select(LOCATION_ID).node().textContent = "* " + formatCoordinates(c[0], c[1]);
                var pointDetails = "* " + formatVector(v[0], v[1]);
                if (overlay) {
                    pointDetails += " | " + formatOverlayValue(overlay(p[0], p[1]));
                }
                d3.select(POINT_DETAILS_ID).node().innerHTML = pointDetails;
            }

        });
    }

    function report(e) {
        log.error(e);
        displayStatus(null, e.error ? e.error == 404 ? "No Data" : e.error + " " + e.message : e);
    }

    var topoTask = loadJson(displayData.topography);
    var dataTask = loadJson(displayData.samples);
    var initTask = when.all([true]).then(apply(init));
    var settingsTask     = when.all([topoTask                             ]).then(apply(createSettings));
    var meshTask         = when.all([topoTask, settingsTask               ]).then(apply(buildMeshes));
    var renderTask       = when.all([settingsTask, meshTask               ]).then(apply(render));
    var plotStationsTask = when.all([dataTask, meshTask                   ]).then(apply(plotStations));
    var overlayTask      = when.all([dataTask, settingsTask, renderTask   ]).then(apply(drawOverlay));
    var fieldTask        = when.all([dataTask, settingsTask, renderTask   ]).then(apply(interpolateField));
    var animateTask      = when.all([settingsTask, fieldTask              ]).then(apply(animate));
    var postInitTask     = when.all([settingsTask, fieldTask, overlayTask ]).then(apply(postInit));

    when.all([
        topoTask,
        dataTask,
        initTask,
        settingsTask,
        meshTask,
        renderTask,
        plotStationsTask,
        overlayTask,
        fieldTask,
        animateTask,
        postInitTask
    ]).then(null, report);

})());
