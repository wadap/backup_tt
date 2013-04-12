#!/usr/bin/env ruby
# -*- coding: utf-8 -*-

require 'rubygems'
require 'fileutils'
require 'find'
require 'time'
require 'pit'
require 'aws/s3'
include AWS::S3

# get config from pit
Config = Pit.get('ttbackup_config',
  :require => {
    'aws' => {
      'access_key_id'     => 'your AWS access key id',
      'secret_access_key' => 'your AWS secret access key',
      'bucket'            => 'your bucket name',
      'region'            => 'your aws region',
    },
  })

# tt setting
TT_HOST    = 'localhost'
TCRMGR     = '/usr/local/tokyo-tyrant/bin/tcrmgr'
ULOG_DIR   = '/var/ttserver/ulog-1'
ULOG_LIMIT = 5

# backup setting
BACKUP_DIR  = '/var/backup'
BACKUP_FILE = 'backup.tch'
PURGE_LIMIT = 5

# exec
def backup
  date_path = Time.now.strftime('%Y/%m/%d')
  dstdir    = "#{BACKUP_DIR}/#{date_path}"
  FileUtils.mkdir_p(dstdir)
  cmd = []
  cmd.push("#{TCRMGR} copy")
  cmd.push(TT_HOST)
  cmd.push("#{dstdir}/#{BACKUP_FILE}")
  system cmd.join(' ')
  "#{dstdir}/#{BACKUP_FILE}"
end

# remove ulog
def remove_ulog
  files = []
  dir = Dir.foreach(ULOG_DIR).each do |v|
    next if v =~ /^\./
    files.push v
  end

  files.sort!

  if files.size > ULOG_LIMIT then
    files[0, files.size - ULOG_LIMIT].each do |file|
      ulog = "#{ULOG_DIR}/#{file}"
      File.unlink ulog if File.exists?(ulog)
    end
  end
end

# remove old backup
def purge_backup
  Find.find(BACKUP_DIR).each do |v|
    fs = File::Stat.new(v)
    if fs.file? then
      result = (Time.now - fs.mtime).divmod(24*60*60)
      File.unlink(v) if (result[0] > PURGE_LIMIT)
    end
  end
end

# put s3
def put_s3(file)
  AWS::S3::Base.establish_connection!(
    :access_key_id     => Config['aws']['access_key_id'],
    :secret_access_key => Config['aws']['secret_access_key']
  )
  AWS::S3::DEFAULT_HOST.replace Config['aws']['region']
  path = file.split(BACKUP_DIR)
  path = path[1]

  if File.exists?(file)
    S3Object.store(
      path,
      open(file),
      Config['aws']['bucket']
    )
  end
end

def main
  file = backup
  put_s3(file)
  remove_ulog
  purge_backup
end

main
