class CreateUploadFiles < ActiveRecord::Migration
  def change
    drop_table :upload_files if ActiveRecord::Base.connection.table_exists? 'upload_files'
    create_table :upload_files do |t|
      t.string :key
      t.string :bucket
      t.string :etag
      t.boolean :success
      t.boolean :s3_connect_success
      t.timestamps
    end
  end
end
