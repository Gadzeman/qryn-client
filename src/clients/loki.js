const { GigapipeError } = require('../types');
const { Stream } = require('../models');
const Http = require('../services/http');

class Loki {
  /**
   * Create a new Loki instance.
   * @param {Http} service - The HTTP service to use for requests.
   */
  constructor(service) {
    this.service = service;
  }

  /**
   * Push streams to Loki.
   * @param {Stream[]} streams - An array of Stream instances to push.
   * @param {Object} options - Additional options for the request.
   * @param {string} options.orgId - The organization ID for the request.
   * @returns {Promise<Object>} The response from the Loki API.
   * @throws {GigapipeError} If the push fails or if the input is invalid.
   */
  async push(streams, options = {}) {

    let payload = { streams: []}
    if (!Array.isArray(streams) || !streams.every(s => {
        if(s instanceof Stream){
          if(s.entries.length)
            payload.streams.push(s.collect())
            return s;
        }
      
    })) {
      throw new GigapipeError('Streams must be an array of Stream instances');
    }
    const headers = this.headers(options);

    try {
      const response = await this.service.request('/loki/api/v1/push', {
        method: 'POST',
        headers,
        body: JSON.stringify(payload)
      });

      streams.forEach(s => s.confirm());
      return response;
    } catch (error) {
      streams.forEach(s => s.undo());
      if (error instanceof GigapipeError) {
        throw error;
      }
      throw new GigapipeError(`Loki push failed: ${error.message}`, error.statusCode);
    }
  }

  /**
   * Execute a LogQL query and retrieve log results.
   * @param {string} query - The LogQL query string.
   * @param {Object} options - Additional options for the request.
   * @param {string} [options.orgId] - The organization ID for the request.
   * @param {number} [options.limit] - The maximum number of entries to return.
   * @param {number} [options.start] - The start timestamp in nanoseconds (Unix epoch).
   * @param {number} [options.end] - The end timestamp in nanoseconds (Unix epoch).
   * @param {boolean} [options.parse=false] - If true, returns parsed logs array instead of raw response.
   * @returns {Promise<QrynResponse|Array>} A promise that resolves to the response from the query endpoint, or parsed logs array if parse option is true.
   * @throws {GigapipeError} If the query request fails.
   */
  async query(query, options = {}) {
    if (!query) {
      throw new GigapipeError('Query parameter is required');
    }

    const params = new URLSearchParams({ query });
    if (options.limit) params.append('limit', options.limit);
    if (options.start) params.append('start', options.start);
    if (options.end) params.append('end', options.end);

    const result = await this.service.request(`/loki/api/v1/query?${params.toString()}`, {
      method: 'GET',
      headers: this.headers(options)
    }).catch(error => {
      if (error instanceof GigapipeError) {
        throw error;
      }
      throw new GigapipeError(`Loki query failed: ${error.message}`, error.statusCode);
    });

    if (options.parse) {
      return Loki.parseLogs(result);
    }

    return result;
  }

  /**
   * Execute a LogQL query over a range of time.
   * @param {string} query - The LogQL query string.
   * @param {number} start - The start timestamp in nanoseconds (Unix epoch).
   * @param {number} end - The end timestamp in nanoseconds (Unix epoch).
   * @param {Object} options - Additional options for the request.
   * @param {string} [options.orgId] - The organization ID for the request.
   * @param {number} [options.step] - The query resolution step width in nanoseconds.
   * @param {number} [options.limit] - The maximum number of entries to return.
   * @param {boolean} [options.parse=false] - If true, returns parsed logs array instead of raw response.
   * @returns {Promise<QrynResponse|Array>} A promise that resolves to the response from the query range endpoint, or parsed logs array if parse option is true.
   * @throws {GigapipeError} If the query range request fails.
   */
  async queryRange(query, start, end, options = {}) {
    if (!query) {
      throw new GigapipeError('Query parameter is required');
    }
    if (!start || !end) {
      throw new GigapipeError('Start and end timestamps are required');
    }

    const params = new URLSearchParams({ query, start, end });
    if (options.step) params.append('step', options.step);
    if (options.limit) params.append('limit', options.limit);

    const result = await this.service.request(`/loki/api/v1/query_range?${params.toString()}`, {
      method: 'GET',
      headers: this.headers(options)
    }).catch(error => {
      if (error instanceof GigapipeError) {
        throw error;
      }
      throw new GigapipeError(`Loki query range failed: ${error.message}`, error.statusCode);
    });

    if (options.parse) {
      return Loki.parseLogs(result);
    }

    return result;
  }

  /**
   * Retrieve the list of label names.
   * @param {Object} options - Additional options for the request.
   * @param {string} [options.orgId] - The organization ID for the request.
   * @param {number} [options.start] - The start timestamp in nanoseconds (Unix epoch).
   * @param {number} [options.end] - The end timestamp in nanoseconds (Unix epoch).
   * @returns {Promise<QrynResponse>} A promise that resolves to the response from the labels endpoint.
   * @throws {GigapipeError} If the labels request fails.
   */
  async labels(options = {}) {
    const params = new URLSearchParams();
    if (options.start) params.append('start', options.start);
    if (options.end) params.append('end', options.end);

    const queryString = params.toString();
    const url = `/loki/api/v1/labels${queryString ? `?${queryString}` : ''}`;

    return this.service.request(url, {
      method: 'GET',
      headers: this.headers(options)
    }).catch(error => {
      if (error instanceof GigapipeError) {
        throw error;
      }
      throw new GigapipeError(`Loki labels retrieval failed: ${error.message}`, error.statusCode);
    });
  }

  /**
   * Retrieve the list of label values for a specific label name.
   * @param {string} labelName - The name of the label.
   * @param {Object} options - Additional options for the request.
   * @param {string} [options.orgId] - The organization ID for the request.
   * @param {number} [options.start] - The start timestamp in nanoseconds (Unix epoch).
   * @param {number} [options.end] - The end timestamp in nanoseconds (Unix epoch).
   * @returns {Promise<QrynResponse>} A promise that resolves to the response from the label values endpoint.
   * @throws {GigapipeError} If the label values request fails.
   */
  async labelValues(labelName, options = {}) {
    if (!labelName) {
      throw new GigapipeError('Label name parameter is required');
    }

    const params = new URLSearchParams();
    if (options.start) params.append('start', options.start);
    if (options.end) params.append('end', options.end);

    const queryString = params.toString();
    const url = `/loki/api/v1/label/${labelName}/values${queryString ? `?${queryString}` : ''}`;

    return this.service.request(url, {
      method: 'GET',
      headers: this.headers(options)
    }).catch(error => {
      if (error instanceof GigapipeError) {
        throw error;
      }
      throw new GigapipeError(`Loki label values retrieval failed: ${error.message}`, error.statusCode);
    });
  }

  /**
   * Retrieve the list of series that match a specified label set.
   * @param {string[]|string} match - The label matchers (e.g., ['{job="api"}', '{env="prod"}'] or '{job="api"}')
   * @param {Object} options - Additional options for the request.
   * @param {string} [options.orgId] - The organization ID for the request.
   * @param {number} [options.start] - The start timestamp in nanoseconds (Unix epoch).
   * @param {number} [options.end] - The end timestamp in nanoseconds (Unix epoch).
   * @returns {Promise<QrynResponse>} A promise that resolves to the response from the series endpoint.
   * @throws {GigapipeError} If the series request fails.
   */
  async series(match, options = {}) {
    if (!match) {
      throw new GigapipeError('Match parameter is required');
    }

    const params = new URLSearchParams();
    if (options.start) params.append('start', options.start);
    if (options.end) params.append('end', options.end);

    if (typeof match === 'string') {
      params.append('match[]', match);
    } else if (Array.isArray(match)) {
      match.forEach(m => params.append('match[]', m));
    } else {
      throw new GigapipeError('Match must be a string or array of strings');
    }

    return this.service.request(`/loki/api/v1/series?${params.toString()}`, {
      method: 'GET',
      headers: this.headers(options)
    }).catch(error => {
      if (error instanceof GigapipeError) {
        throw error;
      }
      throw new GigapipeError(`Loki series retrieval failed: ${error.message}`, error.statusCode);
    });
  }

  /**
   * Parse query or queryRange result and extract all logs as a flat array.
   * Works with both "streams" (query) and "matrix" (queryRange) result types.
   * @param {QrynResponse} result - The response from query or queryRange.
   * @returns {Array} Array of log objects with timestamp, date, and message (always parsed).
   */
  parseLogs(result) {
    return Loki.parseLogs(result);
  }

  /**
   * Static method to parse query or queryRange result and extract all logs as a flat array.
   * Works with both "streams" (query) and "matrix" (queryRange) result types.
   * @param {QrynResponse} result - The response from query or queryRange.
   * @returns {Array} Array of log objects with timestamp, date, and message (always parsed).
   */
  static parseLogs(result) {
    const logs = [];

    if (!result?.response?.data?.result) {
      return logs;
    }

    const resultType = result.response.data.resultType;
    const results = result.response.data.result;

    if (resultType === 'streams') {
      // query() returns streams format
      results.forEach((stream) => {
        if (!stream.values) return;

        stream.values.forEach((entry) => {
          if (!Array.isArray(entry) || entry.length < 2) return;

          const [timestampNs, message] = entry;

          // Convert nanoseconds to milliseconds
          const timestampMs = parseInt(timestampNs) / 1000000;
          const date = new Date(timestampMs);

          // Try to parse message as JSON, fallback to string
          let parsedMessage;
          try {
            parsedMessage = JSON.parse(message);
          } catch (e) {
            parsedMessage = message;
          }

          logs.push({
            timestamp: timestampNs,
            timestampMs: timestampMs,
            date: date,
            dateISO: date.toISOString(),
            message: parsedMessage,
            labels: stream.stream || {}
          });
        });
      });
    } else if (resultType === 'matrix') {
      // queryRange() returns matrix format
      results.forEach((series) => {
        if (!series.values) return;

        series.values.forEach((bucket) => {
          const [, logEntries] = bucket;

          // Handle single entry or array of entries
          const entries = Array.isArray(logEntries) && Array.isArray(logEntries[0])
              ? logEntries  // Multiple entries in this bucket
              : [logEntries]; // Single entry in this bucket

          entries.forEach((entry) => {
            if (!Array.isArray(entry) || entry.length < 2) return;

            const [timestampNs, message] = entry;

            // Convert nanoseconds to milliseconds
            const timestampMs = parseInt(timestampNs) / 1000000;
            const date = new Date(timestampMs);

            // Try to parse message as JSON, fallback to string
            let parsedMessage;
            try {
              parsedMessage = JSON.parse(message);
            } catch (e) {
              parsedMessage = message;
            }

            logs.push({
              timestamp: timestampNs,
              timestampMs: timestampMs,
              date: date,
              dateISO: date.toISOString(),
              message: parsedMessage,
              labels: series.metric || {}
            });
          });
        });
      });
    }

    return logs;
  }

  headers(options = {}) {
    const headers = {};
    if (options.orgId) headers['X-Scope-OrgID'] = options.orgId;
    if (options.async) headers['X-Async-Insert'] = options.async;
    if (options.fpLimit) headers['X-Ttl-Days'] = options.fpLimit;
    if (options.ttlDays) headers['X-FP-LIMIT'] = options.ttlDays;
    return headers;
  }
}

module.exports = Loki;