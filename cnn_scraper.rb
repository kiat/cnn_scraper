class Scraper
  require 'nokogiri'
  require 'open-uri'
  require 'date'
  require 'elasticsearch'
  require 'digest/sha1'
  require 'yaml'

  # global variable setup
  @sleep = 1.5
  @baseurl = ""
  @parsing_formats = []
  @prefix = ""

  # elastic search init
  @client = Elasticsearch::Client.new(:log => false)

  def self.get_index_0(stamp, xpath)
    full_path = @baseurl+"/TRANSCRIPTS/#{stamp.strftime("%Y.%m.%d")}.html"

    doc = Nokogiri::HTML(open(full_path))

    output = {}
    current_topic = ""
    lines = doc.xpath(xpath)

    lines.children.each { |m|

      current_topic = m.text if m.name == "h3"
      output[current_topic] ||= [] unless current_topic==""

      if m.name == "li"
        m.children.each{ |l|
          current_topic = l.text if l.name=="h3"
          output[current_topic] << {
              :text => l.text.to_s,
              :url => l.attribute('href').to_s
          } if l.name=="a"
        }
      end

    }
    output
  end


  def self.get_index_1(stamp, xpath)
    full_path = @baseurl+"/TRANSCRIPTS/#{stamp.strftime("%Y.%m.%d")}.html"

    doc = Nokogiri::HTML(open(full_path))

    output = {}
    current_topic = ""
    lines = doc.xpath(xpath)

    lines.children.each { |m|

      current_topic = m.text if m.name == "h3"
      output[current_topic] ||= [] unless current_topic==""

      if m.name == "ul"
        m.xpath("li").children.each{ |l|
          output[current_topic] << {
              :text => l.text.to_s,
              :url => @baseurl+l.attribute('href').to_s
          } if l.name=="a"
        }
      end
    }
    output
  end


  def self.get_index_2(stamp, xpath)
    full_path = @baseurl+"/TRANSCRIPTS/#{stamp.strftime("%Y.%m.%d")}.html"

    doc = Nokogiri::HTML(open(full_path))

    output = {}
    current_topic = ""

    lines = doc.xpath(xpath)
    lines.children.each{ |m|

      current_topic = m.text if m.attribute('class').to_s=='cnnTransDate'
      output[current_topic] ||= [] unless current_topic==""

      if m.attribute('class').to_s=='cnnSectBulletItems'
        articles = m.css("a")

        articles.each{|a|
          output[current_topic] << {
              :text => a.text.to_s,
              :url => @baseurl+a.attribute('href').to_s
          }
        }
      end
    }

    output
  end

  def self.get_index(stamp, xpath)
    full_path = @baseurl+"/TRANSCRIPTS/#{stamp.strftime("%Y.%m.%d")}.html"

    doc = Nokogiri::HTML(open(full_path))

    output = {}
    current_topic = ""
    lines = doc.css(xpath)

    lines.children.each{ |m|

      current_topic = m.text if m.attribute('class').to_s=='cnnTransDate'
      output[current_topic] ||= [] unless current_topic==""

      if m.attribute('class').to_s=='cnnSectBulletItems'
        articles = m.css("a")

        articles.each{|a|
          output[current_topic] << {
              :text => a.text.to_s,
              :url => @baseurl+a.attribute('href').to_s
          }
        }
      end
    }

    output
  end


  def self.extract_content(aired_raw, content_raw)

    # get metadata
    aired_text = aired_raw
    aired = aired_text.gsub(/[^0-9a-z :]/i, '').gsub(/ {2,}/,' ').strip
    aired = aired.sub 'Aired ', ''
    aired_date = DateTime.parse(aired)

    # retrieve and format content
    raw_content = content_raw.gsub("--",' ')
    raw_content = raw_content.gsub(/ {2,}/,' ')
    raw_content.gsub! /\([A-Za-z\- ]{2,}\)/, ''
    raw_content.gsub /(\n ){2,}/,"\n"
    raw_content = raw_content.gsub "TO ORDER A VIDEO OF THIS TRANSCRIPT, PLEASE CALL 800-CNN-NEWS OR USE OUR SECURE ONLINE ORDER FORM LOCATED AT www.fdch.com", ""
    raw_content = raw_content.gsub "THIS IS A RUSH TRANSCRIPT. THIS COPY MAY NOT BE IN ITS FINAL FORM AND MAY BE UPDATED.", ""
    content = raw_content.split("\n")

    # manage state & create data structure
    current_name = ""
    current_dialog = ""
    output = {
        :metadata => {
            :aired_date => {
                :date => aired_date,
                :date_text => aired_text
            },
            :date => nil,
            :link => "",
            :show => "",
            :snippet => ""
        },
        :content => []
    }

    # extract name and dialog
    content.each{|c|
      if c.length>2
        matches = c.match(/(\b[A-Za-z \',\\.\/\\-]{2,})(: )(.*)/)
        if not matches.nil?
          m = matches.captures
          current_name = m[0]
          current_dialog = m[2]
        else
          current_dialog = c
        end
        output[:content]<<{:name => current_name.strip,:dialog => current_dialog.strip}
      end
    }

    output
  end

  def self.get_transcript_0(url, xpath)
    doc = Nokogiri::HTML(open(url))
    doc.search('br').each {|n| n.replace("\n")}
    lines = doc.xpath(xpath)

    aired = lines.xpath("text()").text.strip
    raw_content = lines.xpath("p")
    content = raw_content.map { |m| m.text}.join("\n")

    extract_content aired, content
  end

  def self.get_transcript_1(url, xpath)
    doc = Nokogiri::HTML(open(url))
    doc.search('br').each {|n| n.replace("\n")}
    lines = doc.xpath(xpath)

    aired = lines.xpath("text()").text.strip
    #puts aired
    content = lines.xpath("table/tr/td").text.strip
    #puts content
    extract_content aired, content
  end

  def self.get_transcript(url, xpath)
    doc = Nokogiri::HTML(open(url))
    doc.search('br').each {|n| n.replace("\n")}
    lines = doc.css(xpath)

    extract_content(lines[0].text,lines[2].text)
  end

  def self.write_out(body)
    id = Digest::SHA1.hexdigest (body[:metadata][:link])
    @client.index({:index => 'cnn', :type => 'transcript', :id => id, :body=> body })
  end

  def self.get_format_index(date)
    @parsing_formats_overides.each_with_index { |item, index|
      return @parsing_formats_overides[index] if item["date"].include? date
    }
    @parsing_formats.each_with_index { |item, index|
      return @parsing_formats[index] if date>=item["start_date"] and date<item["end_date"]
    }
  end

  def self.retrieve_content(stamp)

    begin
      parsing_configs = get_format_index(stamp)



      # get transcripts indexes
      index_id = parsing_configs["index_id"]
      index_path = parsing_configs["index_xpath"]
      transcript_indexes = case index_id
                             when 0
                               get_index_0(stamp,index_path)
                             when 1
                               get_index_1(stamp,index_path)
                             when 2
                               get_index_2(stamp,index_path)
                             else
                               get_index(stamp,index_path)
                           end

      # get transcripts
      transcript_id = parsing_configs["transcript_id"]
      transcript_xpath = parsing_configs["transcript_xpath"]
      if not (transcript_indexes.nil? or transcript_indexes.size==0)
        transcript_indexes.each { |show, transcripts|
          transcripts.each { |t|
            begin
              puts "#{show}\n#{t[:text]}\n#{t[:url]}"

              transcript = case transcript_id
                             when 0
                               get_transcript_0(t[:url],transcript_xpath)
                             when 1
                               get_transcript_1(t[:url], transcript_xpath)
                             else
                               get_transcript(t[:url], transcript_xpath)
                           end

              transcript[:metadata][:link]=t[:url]
              transcript[:metadata][:date]=stamp
              transcript[:metadata][:show]=show
              transcript[:metadata][:snippet]=t[:text]

              write_out(transcript)

            rescue Exception => e
              puts "ERROR: #{e.message}"
              open("log/#{@prefix}error_transcripts.txt",'a'){ |f| f.puts "#{stamp.strftime("%Y.%m.%d")}\t#{t[:url]}\t#{e.message}"}
            end

            puts "---------"
            sleep @sleep

          }
        }
      else
        open("log/#{@prefix}error_empty.txt",'a'){ |f| f.puts "#{stamp.strftime("%Y.%m.%d")}"}
      end

    rescue Exception => e
      puts "Index ERROR: #{e.message}"
      open("log/#{@prefix}index_error.txt",'a'){ |f| f.puts "#{stamp.strftime("%Y.%m.%d")}\t#{e.message}"}
    end

  end

  def self.process
    config = YAML.load_file('formats.yml')

    # setup global variables
    @sleep = config["sleep_secs_between_transcripts"].to_i
    @baseurl = config["baseurl"]
    @parsing_formats = config["formats"]
    @parsing_formats_overides = config["format_overides"]
    @prefix = config["output_file_prefix"]

    #other variables
    start_date = config["start_date"]
    end_date = config["end_date"]

    #retrieve_content(Date.parse('2000-03-29'))

    start_date.upto(end_date) { |date|
      retrieve_content(date)
    }

  end

  process

end
