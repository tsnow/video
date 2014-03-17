S3DirectUploadExample::Application.routes.draw do
  root :to           => 'documents#index'  
  match '/new/'      => 'documents#new',   :via => :POST, :as => :new
  match '/view/'	 => 'documents#view',  :via => :GET,  :as => :view
  match( "/s3/raw-impressions/" => 'impressions#create',
         :via => :POST,
         :as => :create_impressions)
  match( "/s3/:dir" => 'impressions#unimplemented',
         :via => :POST,
         :as => :unimplemented_upload)
  match( "/s3/:dir" => 'impressions#unimplemented',
         :via => :GET,
         :as => :unimplemented_get)
end
