<?php

/* MogileFS.php - Class for accessing the Mogile File System
 * Copyright (C) 2007 Interactive Path, Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/* File Authors:
 *   Erik Osterman <eosterman@interactivepath.com>
 *   Mikhail Mazursky <ash2kk AT gmail>
 *
 * Thanks to the MogileFS mailing list and the creator of the MediaWiki 
 * MogileFS client.
 */

class MogileFS {
    const CMD_DELETE = 'DELETE';
    const CMD_GET_DOMAINS = 'GET_DOMAINS';
    const CMD_GET_PATHS = 'GET_PATHS';
    const CMD_RENAME = 'RENAME';
    const CMD_LIST_KEYS = 'LIST_KEYS';
    const CMD_CREATE_OPEN = 'CREATE_OPEN';
    const CMD_CREATE_CLOSE = 'CREATE_CLOSE';

    const RES_SUCCESS = 'OK';         // Tracker success code
    const RES_ERROR = 'ERR';          // Tracker error code

    const ERR_OTHER = 1000;
    const ERR_UNKNOWN_KEY = 1001;
    const ERR_EMPTY_FILE = 1002;
    const ERR_NONE_MATCH = 1003;

    const DEFAULT_PORT = 7001;        // Tracker port

    protected $_domain;
    protected $_class;
    protected $_trackers;
    protected $_socket;
    protected $_connectTimeout;
    protected $_trackerTimeout;
    protected $_putTimeout;
    protected $_getTimeout;
    protected $_debug;
    protected $_curlInfo;
    protected $_curlError;
    protected $_curlErrno;

    public function __construct($domain, $class, $trackers) {
        $this->setDomain($domain);
        $this->setClass($class);
        $this->setTrackers($trackers);
        $this->setConnectTimeout(3.0);
        $this->setTrackerTimeout(3.0);
        $this->setPutTimeout(10.0);
        $this->setGetTimeout(10.0);
        $this->setDebug(false);
    }

    public function getDebug() {
        return $this->_debug;
    }

    public function setDebug($debug) {
        $this->_debug = (bool) $debug;
    }

    public function getConnectTimeout() {
        return $this->_connectTimeout;
    }

    public function setConnectTimeout($timeout) {
        if ($timeout > 0)
            $this->_connectTimeout = $timeout;
        else
            throw new Exception(get_class($this) . '::setConnectTimeout expects a positive float');
    }

    public function getTrackerTimeout() {
        return $this->_trackerTimeout;
    }

    public function setTrackerTimeout($timeout) {
        if ($timeout > 0)
            $this->_trackerTimeout = $timeout;
        else
            throw new Exception(get_class($this) . '::setTrackerTimeout expects a positive float');
    }

    public function getPutTimeout() {
        return $this->_putTimeout;
    }

    public function setPutTimeout($timeout) {
        if ($timeout > 0)
            $this->_putTimeout = $timeout;
        else
            throw new Exception(get_class($this) . '::setPutTimeout expects a positive float');
    }

    public function getGetTimeout() {
        return $this->_getTimeout;
    }

    public function setGetTimeout($timeout) {
        if ($timeout > 0)
            $this->_getTimeout = $timeout;
        else
            throw new Exception(get_class($this) . '::setGetTimeout expects a positive float');
    }

    public function getTrackers() {
        return $this->_trackers;
    }

    public function setTrackers($trackers) {
        if (is_scalar($trackers))
            $this->_trackers = Array($trackers);
        elseif (is_array($trackers))
            $this->_trackers = $trackers;
        else
            throw new Exception(get_class($this) . '::setTrackers unrecognized trackers argument');
    }

    public function getDomain() {
        return $this->_domain;
    }

    public function setDomain($domain) {
        if (is_scalar($domain))
            $this->_domain = $domain;
        else
            throw new Exception(get_class($this) . '::setDomain unrecognized domain argument');
    }

    public function getClass() {
        return $this->_class;
    }

    public function setClass($class) {
        if (is_scalar($class))
            $this->_class = $class;
        else
            throw new Exception(get_class($this) . '::setClass unrecognized class argument');
    }

    public function getCurlInfo() {
        return $this->_curlInfo;
    }

    public function getCurlError() {
        return $this->_curlError;
    }

    public function getCurlErrno() {
        return $this->_curlErrno;
    }

    // Connect to a mogilefsd; scans through the list of daemons and tries to connect one.
    protected function getConnection() {
        if ($this->_socket && is_resource($this->_socket) && !feof($this->_socket))
            return $this->_socket;

        foreach ($this->_trackers as $tracker) {
            $parts = parse_url($tracker);
            $errno = null;
            $errstr = null;
            $this->_socket = fsockopen(
                            $parts['host'],
                            isset($parts['port']) ? $parts['port'] : self::DEFAULT_PORT,
                            $errno,
                            $errstr,
                            $this->_connectTimeout
            );
            if ($this->_socket) {
                stream_set_timeout(
                        $this->_socket,
                        floor($this->_trackerTimeout),
                        ($this->_trackerTimeout - floor($this->_trackerTimeout)) * 1000
                );
                return $this->_socket;
            }
        }
        throw new Exception(get_class($this) . '::getConnection failed to obtain connection');
    }

    // Send a request to mogilefsd and parse the result.
    protected function doRequest($cmd, Array $args = Array()) {
        $args['domain'] = $this->_domain;
        $args['class'] = $this->_class;
        $params = '';
        foreach ($args as $key => $value)
            $params .= '&' . urlencode($key) . '=' . urlencode($value);

        $socket = $this->getConnection();

        $result = fwrite($socket, $cmd . $params . "\n");
        if ($result === false) {
            fclose($socket);
            throw new Exception(get_class($this) . '::doRequest write failed');
        }
        $line = fgets($socket);
        if ($line === false) {
            fclose($socket);
            throw new Exception(get_class($this) . '::doRequest read failed');
        }
        $words = explode(' ', $line);
        if ($words[0] == self::RES_SUCCESS) {
            parse_str(trim($words[1]), $result);
            return $result;
        }
        if ($words[0] != self::RES_ERROR)
            // Something really bad happened - lets close the connection
            fclose($socket);

        if (!isset($words[1]))
            $words[1] = null;

        switch ($words[1]) {
            case 'unknown_key':
                throw new Exception(get_class($this) . "::doRequest unknown_key {$args['key']}", self::ERR_UNKNOWN_KEY);

            case 'empty_file':
                throw new Exception(get_class($this) . "::doRequest empty_file {$args['key']}", self::ERR_EMPTY_FILE);

            case 'none_match':
                throw new Exception(get_class($this) . "::doRequest none_match {$args['key']}", self::ERR_NONE_MATCH);

            default:
                throw new Exception(get_class($this) . '::doRequest ' . trim(urldecode($line)), self::ERR_OTHER);
        }
    }

    // Return a list of domains
    public function getDomains() {
        $this->_curlInfo = null;
        $this->_curlError = null;
        $this->_curlErrno = 0;
        $res = $this->doRequest(self::CMD_GET_DOMAINS);

        $domains = Array();
        for ($i = 1; $i <= $res['domains']; $i++) {
            $dom = 'domain' . $i;
            $classes = Array();
            for ($j = 1; $j <= $res[$dom . 'classes']; $j++)
                $classes[$res[$dom . 'class' . $j . 'name']] = $res[$dom . 'class' . $j . 'mindevcount'];

            $domains[] = Array('name' => $res[$dom], 'classes' => $classes);
        }
        return $domains;
    }

    public function exists($key) {
        try {
            // Get 1 path maximum without verification
            $this->getPaths($key, 1, true);
            return true;
        } catch (Exception $e) {
            if ($e->getCode() == self::ERR_UNKNOWN_KEY)
                return false;

            throw $e;
        }
    }

    // Get an array of paths
    public function getPaths($key, $pathcount = null, $noverify = false) {
        $this->_curlInfo = null;
        $this->_curlError = null;
        $this->_curlErrno = 0;
        if ($key === null)
            throw new Exception(get_class($this) . '::getPaths key cannot be null');

        $args = Array('key' => $key, 'noverify' => (int) (bool) $noverify);
        if ($pathcount)
            $args['pathcount'] = (int) $pathcount;

        $result = $this->doRequest(self::CMD_GET_PATHS, $args);
        unset($result['paths']);
        return $result;
    }

    // Delete a file from system
    public function delete($key) {
        $this->_curlInfo = null;
        $this->_curlError = null;
        $this->_curlErrno = 0;
        if ($key === null)
            throw new Exception(get_class($this) . '::delete key cannot be null');

        $this->doRequest(self::CMD_DELETE, Array('key' => $key));
    }

    // Rename a file
    public function rename($from, $to) {
        $this->_curlInfo = null;
        $this->_curlError = null;
        $this->_curlErrno = 0;
        if ($from === null)
            throw new Exception(get_class($this) . '::rename from key cannot be null');

        if ($to === null)
            throw new Exception(get_class($this) . '::rename to key cannot be null');

        $this->doRequest(self::CMD_RENAME, Array('from_key' => $from, 'to_key' => $to));
    }

    // Rename a file
    public function listKeys($prefix = null, $lastKey = null, $limit = null) {
        $this->_curlInfo = null;
        $this->_curlError = null;
        $this->_curlErrno = 0;
        try {
            return $this->doRequest(self::CMD_LIST_KEYS, Array(
                'prefix' => $prefix,
                'after' => $lastKey,
                'limit' => $limit
            ));
        } catch (Exception $e) {
            if ($e->getCode() == self::ERR_NONE_MATCH)
                return Array();

            throw $e;
        }
    }

    // Get a file from mogstored and return it as a string
    public function get($key) {
        $this->_curlInfo = null;
        $this->_curlError = null;
        $this->_curlErrno = 0;
        if ($key === null)
            throw new Exception(get_class($this) . '::get key cannot be null');

        $paths = $this->getPaths($key, null, true);
        $ch = curl_init();
        if ($ch === false)
            throw new Exception(get_class($this) . '::get curl_init failed');

        $options = Array(
            CURLOPT_VERBOSE => $this->_debug,
            CURLOPT_CONNECTTIMEOUT_MS => $this->_connectTimeout * 1000,
            CURLOPT_TIMEOUT_MS => $this->_getTimeout * 1000,
            CURLOPT_FAILONERROR => true,
            CURLOPT_RETURNTRANSFER => true
        );
        if (!curl_setopt_array($ch, $options)) {
            curl_close($ch);
            throw new Exception(get_class($this) . '::get curl_setopt_array failed');
        }
        foreach ($paths as $path) {
            if (!curl_setopt($ch, CURLOPT_URL, $path)) {
                curl_close($ch);
                throw new Exception(get_class($this) . '::get curl_setopt failed');
            }
            $response = curl_exec($ch);
            $this->_curlInfo = curl_getinfo($ch);
            $this->_curlError = curl_error($ch);
            $this->_curlErrno = curl_errno($ch);
            if ($response === false)
                continue; // Try next source

            curl_close($ch);
            return $response;
        }
        curl_close($ch);
        throw new Exception(get_class($this) . "::get unable to retrieve {$key}");
    }

    // Get a file from mogstored and send it directly to stdout by way of fpassthru()
    public function getPassthru($key) {
        $this->_curlInfo = null;
        $this->_curlError = null;
        $this->_curlErrno = 0;
        if ($key === null)
            throw new Exception(get_class($this) . '::getPassthru key cannot be null');

        $paths = $this->getPaths($key);
        $context = stream_context_create(array('http' => array('timeout' => $this->_connectTimeout)));
        foreach ($paths as $path) {
            $fh = fopen($path, 'rb', false, $context);
            if ($fh === false)
                continue;

            stream_set_timeout(
                        $fh,
                        floor($this->_getTimeout),
                        ($this->_getTimeout - floor($this->_getTimeout)) * 1000
            );
            $result = fpassthru($fh);
            fclose($fh);
            if ($result === false)
                throw new Exception(get_class($this) . '::getPassthru failed');

            return;
        }
        throw new Exception(get_class($this) . "::getPassthru unable to retrieve {$key}");
    }

    // Save a file to the MogileFS
    public function setResource($key, $fh, $length) {
        $this->_curlInfo = null;
        $this->_curlError = null;
        $this->_curlErrno = 0;
        if ($key === null) {
            fclose($fh);
            throw new Exception(get_class($this) . '::setResource key cannot be null');
        }
        $location = $this->doRequest(self::CMD_CREATE_OPEN, Array('key' => $key));
        $ch = curl_init($location['path']);
        if ($ch === false) {
            fclose($fh);
            throw new Exception(get_class($this) . '::setResource curl_init failed');
        }
        $options = Array(
            CURLOPT_VERBOSE => $this->_debug,
            CURLOPT_INFILE => $fh,
            CURLOPT_INFILESIZE => $length,
            CURLOPT_CONNECTTIMEOUT_MS => $this->_connectTimeout * 1000,
            CURLOPT_TIMEOUT_MS => $this->_putTimeout * 1000,
            CURLOPT_PUT => true,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_HTTPHEADER => Array('Expect: ')
        );
        if (!curl_setopt_array($ch, $options)) {
            fclose($fh);
            curl_close($ch);
            throw new Exception(get_class($this) . '::setResource curl_setopt_array failed');
        }
        $response = curl_exec($ch);
        fclose($fh);
        $this->_curlInfo = curl_getinfo($ch);
        $this->_curlError = curl_error($ch);
        $this->_curlErrno = curl_errno($ch);
        curl_close($ch);
        if ($response === false)
            throw new Exception(get_class($this) . "::setResource {$this->_curlError}");

        if ($this->_curlInfo['http_code'] != 201) // Not HTTP 201 Created
            throw new Exception(get_class($this) . "::setResource server returned HTTP {$this->_curlInfo['http_code']} code");

        $this->doRequest(self::CMD_CREATE_CLOSE, Array(
            'key' => $key,
            'devid' => $location['devid'],
            'fid' => $location['fid'],
            'path' => urldecode($location['path'])
        ));
    }

    public function set($key, $value) {
        $this->_curlInfo = null;
        $this->_curlError = null;
        $this->_curlErrno = 0;
        if ($key === null)
            throw new Exception(get_class($this) . '::set key cannot be null');

        $fh = fopen('php://memory', 'rw');
        if ($fh === false)
            throw new Exception(get_class($this) . '::set failed to open memory stream');

        if (fwrite($fh, $value) === false) {
            fclose($fh);
            throw new Exception(get_class($this) . '::set write failed');
        }
        if (!rewind($fh)) {
            fclose($fh);
            throw new Exception(get_class($this) . '::set rewind failed');
        }
        $this->setResource($key, $fh, strlen($value));
    }

    public function setFile($key, $filename) {
        $this->_curlInfo = null;
        $this->_curlError = null;
        $this->_curlErrno = 0;
        if ($key === null)
            throw new Exception(get_class($this) . '::setFile key cannot be null');

        $filesize = filesize($filename);
        if ($filesize === false)
            throw new Exception(get_class($this) . "::setFile failed to get file size of {$filename}");

        $fh = fopen($filename, 'r');
        if ($fh === false)
            throw new Exception(get_class($this) . "::setFile failed to open path {$filename}");

        $this->setResource($key, $fh, $filesize);
    }

}

/*
  // Usage Example:
  $mfs = new MogileFS('socialverse', 'assets', 'tcp://127.0.0.1');
  //$mfs->setDebug(true);
  $start = microtime(true);
  $mfs->set('test123',  microtime(true));
  printf("EXISTS: %d\n", $mfs->exists('test123'));
  print "GET: [" . $mfs->get('test123') . "]\n";
  $mfs->delete('test123');
  $stop = microtime(true);
  printf("%.4f\n", $stop - $start);
 */
