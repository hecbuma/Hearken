#!/usr/bin/env ruby
# frozen_string_literal: true

require_relative '../config/environment'
require 'eventmachine'
require 'mongo'
require 'logger'

module Config
  MAPPING = {
    post: { klass: 'Facebook::TokenCreator', operation: 'update' } # TODO: Adjust the class and operation as necessary
  }.freeze
end

class MongoStreamListener
  class << self
    attr_reader :last_resume_token
  end

  def self.run
    EM.run do
      setup_change_stream
      setup_signal_handlers
    end
  end

  def self.setup_change_stream
    last_resume_token = {"_data"=>'8266301E42000000012B022C0100296E5A10046883646B502343018F22F491AF1EF20C46645F6964006466301CF35B4D3579E635E2620004'}
    db = Mongoid.default_client.database
    options = { full_document: 'updateLookup' }
    options[:start_after] = last_resume_token if last_resume_token

    begin
      change_stream = db.watch([], options)
      change_stream.each do |change|
        handle_change(change)
        update_last_resume_token(change['_id'])
      end
    rescue Mongo::Error::NoServerAvailable, Mongo::Error::SocketError => e
      logger.error("Connection error occurred: #{e.message}. Attempting to reconnect...")
      sleep(10) # Delay before retrying to avoid hammering the server
      retry
    rescue StandardError => e
      logger.error("Error processing change: #{e.message}")
      retry
    end
  end

  def self.handle_change(change)
    doc_type = change.dig('fullDocument', '_type').try(:underscore).try(:to_sym)
    operation = change['operationType']

    validate_change(doc_type, operation)
    process_change(doc_type, change)
  end

  def self.validate_change(doc_type, operation)
    raise 'Unaccepted type' unless Config::MAPPING.key?(doc_type)
    raise 'Unaccepted operation' unless operation == Config::MAPPING[doc_type][:operation]
  end

  def self.process_change(doc_type, change)
    klass = Config::MAPPING[doc_type][:klass].constantize
    klass.new(change['fullDocument']).send(:call)
  end

  def self.setup_signal_handlers
    Signal.trap('INT') do
      logger.info('Shutdown requested (INT signal).')
      EM.stop
    end
    Signal.trap('TERM') do
      logger.info('Shutdown requested (TERM signal).')
      EM.stop
    end
  end

  def self.update_last_resume_token(token)
    @last_resume_token = token
  end

  def self.logger
    @logger ||= Logger.new($stdout)
  end
end

MongoStreamListener.run if __FILE__ == $PROGRAM_NAME
