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
    protected $_putTimeout;
    protected $_getTimeout;
    protected $_debug;

    public function __construct($domain, $class, $trackers) {
        $this->setDomain($domain);
        $this->setClass($class);
        $this->setHosts($trackers);
        $this->setConnectTimeout(3);
        $this->setPutTimeout(10);
        $this->setGetTimeout(10);
        $this->setDebug(false);
    }

    public function getDebug() {
        return $this->_debug;
    }

    public function setDebug($debug) {
        return $this->_debug = (bool) $debug;
    }

    public function getConnectTimeout() {
        return $this->_connectTimeout;
    }

    public function setConnectTimeout($timeout) {
        if ($timeout > 0)
            return $this->_connectTimeout = $timeout;
        else
            throw new Exception(get_class($this) . '::setConnectTimeout expects a positive integer');
    }

    public function getPutTimeout() {
        return $this->_putTimeout;
    }

    public function setPutTimeout($timeout) {
        if ($timeout > 0)
            return $this->_putTimeout = $timeout;
        else
            throw new Exception(get_class($this) . '::setPutTimeout expects a positive integer');
    }

    public function getGetTimeout() {
        return $this->_getTimeout;
    }

    public function setGetTimeout($timeout) {
        if ($timeout > 0)
            return $this->_getTimeout = $timeout;
        else
            throw new Exception(get_class($this) . '::setGetTimeout expects a positive integer');
    }

    public function getHosts() {
        return $this->_trackers;
    }

    public function setHosts($trackers) {
        if (is_scalar($trackers))
            $this->_trackers = Array($trackers);
        elseif (is_array($trackers))
            $this->_trackers = $trackers;
        else
            throw new Exception(get_class($this) . '::setHosts unrecognized trackers argument');
    }

    public function getDomain() {
        return $this->_domain;
    }

    public function setDomain($domain) {
        if (is_scalar($domain))
            return $this->_domain = $domain;
        else
            throw new Exception(get_class($this) . '::setDomain unrecognized domain argument');
    }

    public function getClass() {
        return $this->_class;
    }

    public function setClass($class) {
        if (is_scalar($class))
            return $this->_class = $class;
        else
            throw new Exception(get_class($this) . '::setClass unrecognized class argument');
    }

    // Connect to a mogilefsd; scans through the list of daemons and tries to connect one.
    public function getConnection() {
        if ($this->_socket && is_resource($this->_socket) && !feof($this->_socket))
            return $this->_socket;

        foreach ($this->_trackers as $host) {
            $parts = parse_url($host);
            if (!isset($parts['port']))
                $parts['port'] = self::DEFAULT_PORT;

            $errno = null;
            $errstr = null;
            $this->_socket = fsockopen($parts['host'], $parts['port'], $errno, $errstr, $this->_connectTimeout);
            if ($this->_socket)
                break;
        }

        if (!is_resource($this->_socket) || feof($this->_socket))
            throw new Exception(get_class($this) . '::getConnection failed to obtain connection');
        else
            return $this->_socket;
    }

    // Send a request to mogilefsd and parse the result.
    protected function doRequest($cmd, Array $args = Array()) {
        try {
            $args['domain'] = $this->_domain;
            $args['class'] = $this->_class;
            $params = '';
            foreach ($args as $key => $value)
                $params .= '&' . urlencode($key) . '=' . urlencode($value);

            $socket = $this->getConnection();

            $result = fwrite($socket, $cmd . $params . "\n");
            if ($result === false)
                throw new Exception(get_class($this) . '::doRequest write failed');
            $line = fgets($socket);
            if ($line === false)
                throw new Exception(get_class($this) . '::doRequest read failed');

            $words = explode(' ', $line);
            if ($words[0] == self::RES_SUCCESS) {
                parse_str(trim($words[1]), $result);
                return $result;
            }
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
        } catch (Exception $e) {
            // Clean up
            if (isset($socket))
                fclose($socket);
            // Recast the exception
            throw $e;
        }
    }

    // Return a list of domains
    public function getDomains() {
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
        if ($key === null)
            throw new Exception(get_class($this) . '::exists key cannot be null');

        try {
            $this->doRequest(self::CMD_GET_PATHS, Array('key' => $key));
            return true;
        } catch (Exception $e) {
            return false;
        }
    }

    // Get an array of paths
    public function getPaths($key) {
        if ($key === null)
            throw new Exception(get_class($this) . '::getPaths key cannot be null');

        $result = $this->doRequest(self::CMD_GET_PATHS, Array('key' => $key));
        unset($result['paths']);
        return $result;
    }

    // Delete a file from system
    public function delete($key) {
        if ($key === null)
            throw new Exception(get_class($this) . '::delete key cannot be null');
        $this->doRequest(self::CMD_DELETE, Array('key' => $key));
        return true;
    }

    // Rename a file
    public function rename($from, $to) {
        if ($from === null)
            throw new Exception(get_class($this) . '::rename from key cannot be null');
        elseif ($to === null)
            throw new Exception(get_class($this) . '::rename to key cannot be null');
        $this->doRequest(self::CMD_RENAME, Array('from_key' => $from, 'to_key' => $to));
        return true;
    }

    // Rename a file
    public function listKeys($prefix = null, $lastKey = null, $limit = null) {
        try {
            return $this->doRequest(self::CMD_LIST_KEYS, Array(
                'prefix' => $prefix,
                'after' => $lastKey,
                'limit' => $limit
            ));
        } catch (Exception $e) {
            if ($e->getCode() == self::ERR_NONE_MATCH)
                return Array();
            else
                throw $e;
        }
    }

    // Get a file from mogstored and return it as a string
    public function get($key) {
        if ($key === null)
            throw new Exception(get_class($this) . '::get key cannot be null');
        $paths = $this->getPaths($key);
        foreach ($paths as $path) {
            $contents = '';
            $ch = curl_init();
            curl_setopt($ch, CURLOPT_VERBOSE, $this->_debug);
            curl_setopt($ch, CURLOPT_TIMEOUT, $this->_getTimeout);
            curl_setopt($ch, CURLOPT_URL, $path);
            curl_setopt($ch, CURLOPT_FAILONERROR, true);
            curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
            $response = curl_exec($ch);
            if ($response === false) {
                continue; // Try next source
            }
            curl_close($ch);
            return $response;
        }
        throw new Exception(get_class($this) . "::get unable to retrieve {$key}");
    }

    // Get a file from mogstored and send it directly to stdout by way of fpassthru()
    function getPassthru($key) {
        if ($key === null)
            throw new Exception(get_class($this) . '::getPassthru key cannot be null');
        $paths = $this->getPaths($key);
        foreach ($paths as $path) {
            $fh = fopen($path, 'r');
            if ($fh) {
                if (fpassthru($fh) === false)
                    throw new Exception(get_class($this) . '::getPassthru failed');
                fclose($fh);
            }
            return $success;
        }
        throw new Exception(get_class($this) . "::getPassthru unable to retrieve {$key}");
    }

    // Save a file to the MogileFS
    public function setResource($key, $fh, $length) {
        if ($key === null)
            throw new Exception(get_class($this) . '::setResource key cannot be null');

        $location = $this->doRequest(self::CMD_CREATE_OPEN, Array('key' => $key));
        $uri = $location['path'];
        $parts = parse_url($uri);
        $host = $parts['host'];
        $port = $parts['port'];
        $path = $parts['path'];

        $ch = curl_init();
        curl_setopt($ch, CURLOPT_VERBOSE, $this->_debug);
        curl_setopt($ch, CURLOPT_INFILE, $fh);
        curl_setopt($ch, CURLOPT_INFILESIZE, $length);
        curl_setopt($ch, CURLOPT_TIMEOUT, $this->_putTimeout);
        curl_setopt($ch, CURLOPT_PUT, true);
        curl_setopt($ch, CURLOPT_URL, $uri);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_HTTPHEADER, Array('Expect: '));
        $response = curl_exec($ch);
        fclose($fh);
        if ($response === false) {
            $error = curl_error($ch);
            curl_close($ch);
            throw new Exception(get_class($this) . "::set {$error}");
        }
        curl_close($ch);
        $this->doRequest(self::CMD_CREATE_CLOSE, Array(
            'key' => $key,
            'devid' => $location['devid'],
            'fid' => $location['fid'],
            'path' => urldecode($uri)
        ));

        return true;
    }

    public function set($key, $value) {
        if ($key === null)
            throw new Exception(get_class($this) . '::set key cannot be null');
        $fh = fopen('php://memory', 'rw');
        if ($fh === false)
            throw new Exception(get_class($this) . '::set failed to open memory stream');
        fwrite($fh, $value);
        rewind($fh);
        return $this->setResource($key, $fh, strlen($value));
    }

    public function setFile($key, $filename) {
        if ($key === null)
            throw new Exception(get_class($this) . '::setFile key cannot be null');
        $fh = fopen($filename, 'r');
        if ($fh === false)
            throw new Exception(get_class($this) . "::setFile failed to open path {$filename}");
        return $this->setResource($key, $fh, filesize($filename));
    }

}

/*
  // Usage Example:
  $mfs = new MogileFS('socialverse', 'assets', 'tcp://127.0.0.1');
  //$mfs->setDebug(10);
  $start = microtime(true);
  $mfs->set('test123',  microtime(true));
  printf("EXISTS: %d\n", $mfs->exists('test123'));
  print "GET: [" . $mfs->get('test123') . "]\n";
  $mfs->delete('test123');
  $stop = microtime(true);
  printf("%.4f\n", $stop - $start);
 */
