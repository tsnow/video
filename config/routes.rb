S3DirectUploadExample::Application.routes.draw do
  root :to           => 'documents#index'  
  match '/new/'      => 'documents#new',   :via => :POST, :as => :new
  match '/view/'	 => 'documents#view',  :via => :GET,  :as => :view
  match( "/s3/#{ENV['AMAZON_S3_BUCKET']}" => 'impressions#create',
         :via => :POST,
         :as => :create_impressions)
end
