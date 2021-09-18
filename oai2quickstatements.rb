require 'json'
require 'rexml'
require 'namae'
require 'oai'

DATE_FORMAT = '+%Y-%m-%dT00:00:00Z/11'
DC = 'http://purl.org/dc/elements/1.1/'

client = OAI::Client.new ARGV[0]
periodical_qid = ARGV[1]
options = ARGV[2] ? {set: ARGV[2]} : {}

def reconcile(string)
    clean_string = string.gsub(/[^0-9A-Za-z]/, '')
    uri = URI.parse "https://wikidata.reconci.link/en/api?queries=#{{ 'q1' => { 'query' => clean_string,
                                                                                'limit' => 1 } }.to_json}"
    response = JSON.parse(Net::HTTP.get(uri))
    return nil unless response

    results = response['q1']['result']
    return nil unless results.any?

    results&.first['id']
end

def creators(metadata)
    REXML::XPath.match(metadata,
                       './/dc:creator/text()', {'dc' => DC}
                       ).map{|author| Namae.parse(author.value)[0]&.display_order }
                        .compact
                        .map{|author| "\"#{author}\"" }
end

def full_text(metadata)
    match = REXML::XPath.match(metadata,
                       './/dc:identifier/text()', {'dc' => DC})
                        .find{|id| id.value.start_with?('http')}
    match ? ["\"#{match.value}\""] : []

end

def statement(pid, values)
    values.map{|value| "LAST\t#{pid}\t#{value}"}
          .join("\n") + "\n"
end

def subjects(metadata)
    REXML::XPath.match(metadata,
                './/dc:subject/text()', {'dc' => DC})
                .map{|subject| reconcile(subject.value)}
                .uniq
                .compact
end

def date(metadata)
    REXML::XPath.match(metadata,
                       ".//dc:date/text()", {'dc' => DC}
                       ).map{|value| Date.parse(value.value).strftime(DATE_FORMAT) }
end

client.list_records(options).full.each do |record|
    metadata = record.metadata
    begin
      title = REXML::XPath.first(metadata, './/dc:title/text()', {'dc' => DC}).value
    rescue
      next
    end
    next unless title
    already_exists = reconcile(title)
    next if already_exists

    $stdout.write "CREATE\n"
    $stdout.write statement 'Len', ["\"#{title}\""]
    $stdout.write statement 'P2093', creators(metadata)
    $stdout.write statement 'P1476', ["en:\"#{title}\""]
    $stdout.write statement 'P1433', [periodical_qid]
    $stdout.write statement 'P577', date(metadata)
    $stdout.write statement 'P31', ['Q191067']
    $stdout.write statement 'P953', full_text(metadata)
    $stdout.write statement 'P921', subjects(metadata)
end

