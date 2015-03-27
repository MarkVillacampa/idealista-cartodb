Encoding.default_external = Encoding::UTF_8
Encoding.default_internal = Encoding::UTF_8

require 'faraday'
require 'json'
require 'csv'

# Config

IDEALISTA_API_KEY = ENV['IDEALISTA_API_KEY']
IDEALISTA_HTTP_REFERER = 'http://123'
CARTODB_API_KEY = ENV['CARTODB_API_KEY']
CARTODB_USER = ENV['CARTODB_USER']
CSV_PATH = File.expand_path('~/Desktop/idealista.csv')
CARTODB_TABLE_NAME = 'idealista'

# Idealista API

@idealista_conn = Faraday.new(:url => 'http://www.idealista.com') do |faraday|
  faraday.request  :url_encoded            # form-encode POST params
  faraday.response :logger                  # log requests to STDOUT
  faraday.adapter  Faraday.default_adapter  # make requests with Net::HTTP
end

def get_listings(page = 1)
  query = {
    action: 'json',
    k: IDEALISTA_API_KEY,
    operation: 'rent',
    center: '40.417181,-3.704428',
    distance: 3500,
    numPage: page,
    minPrice: nil,
    maxPrice: 700,
    minSize: nil,
    maxSize: nil,
    flat: true,
    studio: true,
    penthhouse: false,
    chalet: false,
    duplex: false,
    garage: false,
    premises: false,
    office: false,
    room: false,
    minRooms: nil,
    pics: 1,
    since: 'a'
  }.delete_if { |k, v| v.nil? }

  response = @idealista_conn.get '/labs/propertyMap.htm', query do |req|
    req.headers['Referer'] = IDEALISTA_HTTP_REFERER
  end

  JSON.parse response.body
end

# Creating the CSV

headers = %w( abrAddress address agency agentLogo bathrooms condition country description distance district favComment favourite floor hasVideo latitude longitude munipality neighborhood numPhotos operation photosUrl position price propertyCode propertyType propertyTypeCode province region rooms showAddress size subregion thumbnail tipUsuCd url userCode videoType )

# Add custom columns. These are not returned by the api.
headers.concat(%w( added_to_csv_at deleted_from_csv_at comments ))

csv = CSV.open(CSV_PATH, 'r+', headers: true)

# This returns a CSV::Table with all the current data in the csv and moves the current working position to the end of the file
csv_table = csv.read

# CSV#headers returns true if headers will be used but they have not been read
if csv.headers == true
  puts "Adding headers"
  csv << headers
end

# Returns an array with all the property codes currently present in the csv
property_codes = csv_table["propertyCode"]

page = 1
total_pages = nil
loop do
 puts "Getting page #{page}#{ '/' + total_pages.to_s if total_pages }"

 json = get_listings(page)

 total_pages = json[1]["totalPages"]
 listings = json[1]["elementList"]

 listings.each do |listing|

   # Since there is no way to filter by number of rooms in the API, we do it here
   next if listing["rooms"].to_i > 1

   # Add listing to the csv only if is not alredy present
   unless property_codes.include? listing["propertyCode"]
     listing_values = listing.values
     # Add the added_to_csv_at value
     listing_values << Time.now.to_s
     csv << listing_values
   end
 end

 break if page == total_pages
 page += 1
end

csv.close

# CartoDB API

@cartodb_v1_conn = Faraday.new(:url => "https://#{CARTODB_USER}.cartodb.com/api/v1") do |faraday|
  faraday.request  :multipart
  faraday.request  :url_encoded            # form-encode POST params
  faraday.response :logger                  # log requests to STDOUT
  faraday.adapter  Faraday.default_adapter  # make requests with Net::HTTP
  faraday.proxy 'http://localhost:8888'
end

@cartodb_v2_conn = Faraday.new(:url => "https://#{CARTODB_USER}.cartodb.com/api/v1") do |faraday|
  faraday.request  :url_encoded            # form-encode POST params
  faraday.response :logger                  # log requests to STDOUT
  faraday.adapter  Faraday.default_adapter  # make requests with Net::HTTP
end

def ids_from_table_with_name(name)
  response = @cartodb_v1_conn.get "viz/?tag_name=&q=&page=1&type=table&per_page=20&tags=&order=updated_at&o%5Bupdated_at%5D=desc&api_key=#{CARTODB_API_KEY}" do |req|
	req['User-Agent'] = "(Macintosh; Mac OS X 10.9.2; en_US)"
	end
  json = JSON.parse response.body

  visualization = json['visualizations'].detect { |v| v['name'] == name }

  { visualization: visualization['id'],
    table: visualization['table']['id'],
    map: visualization['map_id'] }
end

def delete_visualization(id)
  @cartodb_v1_conn.delete "viz/#{id}?api_key=#{CARTODB_API_KEY}"
end

def get_layer_id(map_id)
  response = @cartodb_v1_conn.get "maps/#{map_id}/layers/?api_key=#{CARTODB_API_KEY}"
  json = JSON.parse response.body
  json['layers'].detect { |l| l['kind'] == 'carto' }['id']
end

def upload_csv
  # Note: file upload only works if the file parameter name is 'file' or 'filename'
  response = @cartodb_v1_conn.post "imports/?api_key=#{CARTODB_API_KEY}", { file: Faraday::UploadIO.new(CSV_PATH, 'text/csv') }
  json = JSON.parse response.body

  return unless json['success']

  item_queue_id = json['item_queue_id']

  loop do
    response = @cartodb_v1_conn.get "imports/#{item_queue_id}/?api_key=#{CARTODB_API_KEY}"
    json = JSON.parse response.body
    break if json['state'] == 'complete'
    sleep 2
  end
end

def set_table_options
  columns = {
    "price" => "number",
    "added_to_csv_at" => "date",
    "size" => "number",
    "rooms" => "number"
  }

  columns.each do |k,v|
    @cartodb_v1_conn.put "tables/#{CARTODB_TABLE_NAME}/columns/#{k}?api_key=#{CARTODB_API_KEY}" do |req|
      req['Content-Type'] = 'application/json'
      req.body = %Q({
                      "name": "#{k}",
                      "type": "#{v}"
                    })
    end
  end
end

ids = ids_from_table_with_name(CARTODB_TABLE_NAME)
delete_visualization(ids[:visualization])
upload_csv
