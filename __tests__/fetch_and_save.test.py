import unittest
from unittest.mock import patch, Mock, MagicMock, call
import sys
import os
import time
from pymongo import MongoClient, UpdateOne

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from fetch_and_save import compare_and_sync, fetch_data_and_save_to_mongo


class TestCompareAndSync(unittest.TestCase):
    
    def setUp(self):
        self.mock_collection = Mock()
        
    def test_compare_and_sync_with_empty_results(self):
        """Test compare_and_sync with empty results."""
        # Mock existing documents in collection
        self.mock_collection.find.return_value = [{"_id": "existing1"}, {"_id": "existing2"}]
        
        compare_and_sync(self.mock_collection, [])
        
        # Should remove existing documents by marking them as deleted
        self.mock_collection.update_many.assert_called_once_with(
            {"_id": {"$in": ["existing1", "existing2"]}},
            {"$set": {"deleted": True}}
        )
    
    def test_compare_and_sync_with_missing_ids(self):
        """Test compare_and_sync when some results are missing _id."""
        results = [
            {"_id": "1", "title": "Property 1"},
            {"title": "Property without ID"},
            {"_id": "2", "title": "Property 2"}
        ]
        
        self.mock_collection.find.return_value = []
        self.mock_collection.bulk_write.return_value = Mock(upserted_count=2, modified_count=0, upserted_ids={})
        
        with patch('builtins.print') as mock_print:
            compare_and_sync(self.mock_collection, results)
            
        # Should print warning about missing IDs
        mock_print.assert_any_call("Warning: Some results are missing an '_id' and will be skipped.")
    
    def test_compare_and_sync_removes_old_documents(self):
        """Test that old documents are marked as deleted."""
        results = [{"_id": "1", "title": "Property 1", "mLastUpdate": "2023-01-01"}]
        
        # Mock existing documents in collection
        self.mock_collection.find.return_value = [
            {"_id": "1"}, 
            {"_id": "2"}, 
            {"_id": "3"}
        ]
        self.mock_collection.bulk_write.return_value = Mock(upserted_count=0, modified_count=1, upserted_ids={})
        
        compare_and_sync(self.mock_collection, results)
        
        # Should mark documents 2 and 3 as deleted
        self.mock_collection.update_many.assert_called_with(
            {"_id": {"$in": ["2", "3"]}},
            {"$set": {"deleted": True}}
        )
    
    def test_compare_and_sync_upserts_new_documents(self):
        """Test that new documents are upserted."""
        results = [
            {"_id": "1", "title": "Property 1", "mLastUpdate": "2023-01-01"},
            {"_id": "2", "title": "Property 2", "mLastUpdate": "2023-01-02"}
        ]
        
        self.mock_collection.find.return_value = []
        mock_result = Mock()
        mock_result.upserted_count = 2
        mock_result.modified_count = 0
        mock_result.upserted_ids = {0: "1", 1: "2"}
        self.mock_collection.bulk_write.return_value = mock_result
        
        compare_and_sync(self.mock_collection, results)
        
        # Should call bulk_write twice - once for upserts, once for create dates
        self.assertEqual(self.mock_collection.bulk_write.call_count, 2)
        
        # Check first bulk_write call (upserts)
        first_call = self.mock_collection.bulk_write.call_args_list[0]
        operations = first_call[0][0]
        self.assertEqual(len(operations), 2)
        
        # Verify operations are UpdateOne instances
        for op in operations:
            self.assertIsInstance(op, UpdateOne)
    
    def test_compare_and_sync_removes_mlastupdate_field(self):
        """Test that mLastUpdate field is removed from results."""
        results = [{"_id": "1", "title": "Property 1", "mLastUpdate": "2023-01-01"}]
        
        self.mock_collection.find.return_value = []
        self.mock_collection.bulk_write.return_value = Mock(upserted_count=1, modified_count=0, upserted_ids={0: "1"})
        
        compare_and_sync(self.mock_collection, results)
        
        # Verify mLastUpdate was removed from the result
        self.assertNotIn("mLastUpdate", results[0])
    
    def test_compare_and_sync_sets_create_date_for_new_documents(self):
        """Test that mCreateDate is set for newly inserted documents."""
        results = [{"_id": "1", "title": "Property 1", "mLastUpdate": "2023-01-01"}]
        
        self.mock_collection.find.return_value = []
        mock_result = Mock()
        mock_result.upserted_count = 1
        mock_result.modified_count = 0
        mock_result.upserted_ids = {0: "1"}
        self.mock_collection.bulk_write.return_value = mock_result
        
        with patch('time.time', return_value=1234567890):
            compare_and_sync(self.mock_collection, results)
        
        # Should call bulk_write twice
        self.assertEqual(self.mock_collection.bulk_write.call_count, 2)
        
        # Check second bulk_write call (create dates)
        second_call = self.mock_collection.bulk_write.call_args_list[1]
        create_date_operations = second_call[0][0]
        self.assertEqual(len(create_date_operations), 1)
        
        # Verify the operation sets mCreateDate
        op = create_date_operations[0]
        self.assertIsInstance(op, UpdateOne)


class TestFetchDataAndSaveToMongo(unittest.TestCase):
    
    @patch('fetch_and_save.MongoClient')
    @patch('fetch_and_save.ListingFetcher')
    @patch('fetch_and_save.compare_and_sync')
    def test_fetch_data_and_save_to_mongo_success(self, mock_compare_sync, mock_listing_fetcher, mock_mongo_client):
        """Test successful data fetch and save."""
        # Setup mocks
        mock_fetcher_instance = Mock()
        mock_fetcher_instance.fetch_all_listings.return_value = [{"_id": "1", "title": "Property 1"}]
        mock_listing_fetcher.return_value = mock_fetcher_instance
        
        mock_client = Mock()
        mock_db = Mock()
        mock_collection = Mock()
        mock_client.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection
        mock_mongo_client.return_value = mock_client
        
        fetch_data_and_save_to_mongo()
        
        # Verify ListingFetcher was called with correct URL
        mock_listing_fetcher.assert_called_once_with('https://www.immobiliare.it/api-next/search-list/listings/?fkRegione=fri&idProvincia=UD&idNazione=IT&idContratto=2&idCategoria=1&prezzoMassimo=1200&__lang=it&minLat=46.048872&maxLat=46.07978&minLng=13.189259&maxLng=13.273544&pag=1&paramsCount=5&path=%2Faffitto-case%2Fudine-provincia%2F')
        
        # Verify MongoDB connection
        mock_mongo_client.assert_called_once()
        
        # Verify compare_and_sync was called
        mock_compare_sync.assert_called_once_with(mock_collection, [{"_id": "1", "title": "Property 1"}])
        
        # Verify client was closed
        mock_client.close.assert_called_once()
    
    @patch('fetch_and_save.MongoClient')
    @patch('fetch_and_save.ListingFetcher')
    @patch('fetch_and_save.compare_and_sync')
    def test_fetch_data_and_save_to_mongo_empty_results(self, mock_compare_sync, mock_listing_fetcher, mock_mongo_client):
        """Test handling of empty results."""
        # Setup mocks
        mock_fetcher_instance = Mock()
        mock_fetcher_instance.fetch_all_listings.return_value = []
        mock_listing_fetcher.return_value = mock_fetcher_instance
        
        mock_client = Mock()
        mock_db = Mock()
        mock_collection = Mock()
        mock_client.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection
        mock_mongo_client.return_value = mock_client
        
        with patch('builtins.print') as mock_print:
            fetch_data_and_save_to_mongo()
        
        # Should still call compare_and_sync with empty results
        mock_compare_sync.assert_called_once_with(mock_collection, [])
        
        # Verify client was closed
        mock_client.close.assert_called_once()
    
    @patch('fetch_and_save.ListingFetcher')
    def test_fetch_data_and_save_to_mongo_request_exception(self, mock_listing_fetcher):
        """Test handling of request exceptions."""
        # Setup mock to raise RequestException
        mock_fetcher_instance = Mock()
        mock_fetcher_instance.fetch_all_listings.side_effect = Exception("Network error")
        mock_listing_fetcher.return_value = mock_fetcher_instance
        
        with patch('builtins.print') as mock_print:
            fetch_data_and_save_to_mongo()
        
        # Should print error message
        mock_print.assert_any_call("An error occurred: Network error")
    
    @patch('fetch_and_save.MongoClient')
    @patch('fetch_and_save.ListingFetcher')
    def test_fetch_data_and_save_to_mongo_mongo_exception(self, mock_listing_fetcher, mock_mongo_client):
        """Test handling of MongoDB connection exceptions."""
        # Setup mocks
        mock_fetcher_instance = Mock()
        mock_fetcher_instance.fetch_all_listings.return_value = [{"_id": "1", "title": "Property 1"}]
        mock_listing_fetcher.return_value = mock_fetcher_instance
        
        # Make MongoDB connection fail
        mock_mongo_client.side_effect = Exception("MongoDB connection failed")
        
        with patch('builtins.print') as mock_print:
            fetch_data_and_save_to_mongo()
        
        # Should print error message
        mock_print.assert_any_call("An error occurred: MongoDB connection failed")
    
    @patch('fetch_and_save.MongoClient')
    @patch('fetch_and_save.ListingFetcher')
    @patch('fetch_and_save.compare_and_sync')
    def test_fetch_data_and_save_to_mongo_client_cleanup(self, mock_compare_sync, mock_listing_fetcher, mock_mongo_client):
        """Test that MongoDB client is properly cleaned up even when compare_and_sync fails."""
        # Setup mocks
        mock_fetcher_instance = Mock()
        mock_fetcher_instance.fetch_all_listings.return_value = [{"_id": "1", "title": "Property 1"}]
        mock_listing_fetcher.return_value = mock_fetcher_instance
        
        mock_client = Mock()
        mock_db = Mock()
        mock_collection = Mock()
        mock_client.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection
        mock_mongo_client.return_value = mock_client
        
        # Make compare_and_sync fail
        mock_compare_sync.side_effect = Exception("Sync failed")
        
        with patch('builtins.print') as mock_print:
            fetch_data_and_save_to_mongo()
        
        # Verify client was still closed despite the error
        mock_client.close.assert_called_once()
        mock_print.assert_any_call("An error occurred: Sync failed")
        mock_print.assert_any_call("MongoDB connection closed.")
    
    @patch('fetch_and_save.MongoClient')
    @patch('fetch_and_save.ListingFetcher')
    @patch('fetch_and_save.compare_and_sync')  
    def test_fetch_data_and_save_to_mongo_no_client_variable(self, mock_compare_sync, mock_listing_fetcher, mock_mongo_client):
        """Test cleanup when client variable doesn't exist (early failure)."""
        # Make ListingFetcher fail before client is created
        mock_listing_fetcher.side_effect = Exception("Fetcher failed")
        
        with patch('builtins.print') as mock_print:
            fetch_data_and_save_to_mongo()
        
        # MongoDB client should not be called or closed
        mock_mongo_client.assert_not_called()
        mock_print.assert_any_call("An error occurred: Fetcher failed")


class TestIntegration(unittest.TestCase):
    """Integration tests for the entire workflow."""
    
    @patch('fetch_and_save.MongoClient')
    @patch('fetch_and_save.ListingFetcher')
    def test_full_workflow_integration(self, mock_listing_fetcher, mock_mongo_client):
        """Test the complete workflow from fetch to save."""
        # Setup realistic test data
        test_results = [
            {"_id": "prop1", "title": "House 1", "price": 1000, "mLastUpdate": "2023-01-01"},
            {"_id": "prop2", "title": "House 2", "price": 1200, "mLastUpdate": "2023-01-02"}
        ]
        
        # Setup mocks
        mock_fetcher_instance = Mock()
        mock_fetcher_instance.fetch_all_listings.return_value = test_results
        mock_listing_fetcher.return_value = mock_fetcher_instance
        
        mock_client = Mock()
        mock_db = Mock()
        mock_collection = Mock()
        mock_client.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection
        mock_mongo_client.return_value = mock_client
        
        # Mock collection behavior
        mock_collection.find.return_value = [{"_id": "old_prop"}]  # Existing document
        mock_result = Mock()
        mock_result.upserted_count = 2
        mock_result.modified_count = 0
        mock_result.upserted_ids = {0: "prop1", 1: "prop2"}
        mock_collection.bulk_write.return_value = mock_result
        
        with patch('time.time', return_value=1234567890):
            fetch_data_and_save_to_mongo()
        
        # Verify the workflow
        # 1. Data was fetched
        mock_fetcher_instance.fetch_all_listings.assert_called_once()
        
        # 2. MongoDB connection was established
        mock_mongo_client.assert_called_once()
        
        # 3. Old documents were marked as deleted
        mock_collection.update_many.assert_called_once_with(
            {"_id": {"$in": ["old_prop"]}},
            {"$set": {"deleted": True}}
        )
        
        # 4. New documents were upserted (2 calls to bulk_write)
        self.assertEqual(mock_collection.bulk_write.call_count, 2)
        
        # 5. Client was closed
        mock_client.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()