<?php 
require_once('riak-php-client/src/Basho/Riak/Riak.php');
require_once('riak-php-client/src/Basho/Riak/Bucket.php');
require_once('riak-php-client/src/Basho/Riak/Exception.php');
require_once('riak-php-client/src/Basho/Riak/Link.php');
require_once('riak-php-client/src/Basho/Riak/MapReduce.php');
require_once('riak-php-client/src/Basho/Riak/Object.php');
require_once('riak-php-client/src/Basho/Riak/StringIO.php');
require_once('riak-php-client/src/Basho/Riak/Utils.php');
require_once('riak-php-client/src/Basho/Riak/Link/Phase.php');
require_once('riak-php-client/src/Basho/Riak/MapReduce/Phase.php');

// Class definitions for our models
class Customer {
    var $customerId;
    var $name;
    var $address;
    var $city;
    var $state;
    var $zip;
    var $phone;
    var $createdDate;
}

class Order {
    public function __construct() {
        $this->items = array();
    }
    var $orderId;
    var $customerId;
    var $salespersonId;
    var $items;
    var $total;
    var $orderDate;
}

class Item {
    public function __construct($itemId, $title, $price) {
        $this->itemId = $itemId;
        $this->title = $title;
        $this->price = $price;
    }
    var $itemId;
    var $title;
    var $price;
}

class OrderSummary {
    public function __construct() {
        $this->summaries = array();
    }
    var $customerId;
    var $summaries;
}

class OrderSummaryItem {
    public function __construct(Order $order) {
        $this->orderId = $order->orderId;
        $this->total = $order->total;
        $this->orderDate = $order->orderDate;
    }
    var $orderId;
    var $total;
    var $orderDate;
}


// Creating Data
$customer = new Customer();
$customer->customerId = 1;
$customer->name = 'John Smith';
$customer->address = '123 Main Street';
$customer->city = 'Columbus';
$customer->state = 'Ohio';
$customer->zip = '43210';
$customer->phone = '+1-614-555-5555';
$customer->createdDate = '2013-10-01 14:30:26';


$orders = array();

$order1 = new Order();
$order1->orderId = 1;
$order1->customerId = 1;
$order1->salespersonId = 9000;
$order1->items = array(
        new Item('TCV37GIT4NJ',
                'USB 3.0 Coffee Warmer',
                15.99),
        new Item('PEG10BBF2PP', 
                 'eTablet Pro; 24GB; Grey', 
                 399.99));
$order1->total = 415.98;
$order1->orderDate = '2013-10-01 14:42:26';
$orders[] = $order1;

$order2 = new Order();
$order2->orderId = 2;
$order2->customerId = 1;
$order2->salespersonId = 9001;
$order2->items = array(
        new Item('OAX19XWN0QP',
                'GoSlo Digital Camera',
                359.99));
$order2->total = 359.99;
$order2->orderDate = '2013-10-15 16:43:16';
$orders[] = $order2;

$order3 = new Order();
$order3->orderId = 3;
$order3->customerId = 1;
$order3->salespersonId = 9000;
$order3->items = array(
        new Item('WYK12EPU5EZ',
                'Call of Battle = Goats - Gamesphere 4',
                69.99),
        new Item('TJB84HAA8OA',
                'Bricko Building Blocks',
                4.99));
$order3->total = 74.98;
$order3->orderDate = '2013-11-03 17:45:28';
$orders[] = $order3;


$orderSummary = new OrderSummary();
$orderSummary->customerId = 1;
foreach ($orders as $order) {
    $orderSummary->summaries[] = new OrderSummaryItem($order);
}
unset($order);


// Starting Client
$client = new Basho\Riak\Riak('127.0.0.1', 10018);

// Creating Buckets
$customersBucket = $client->bucket('Customers');
$ordersBucket = $client->bucket('Orders');
$orderSummariesBucket = $client->bucket('OrderSummaries');


// Storing Data
$customer_riak = $customersBucket->newObject(strval($customer->customerId), $customer);
$customer_riak->store();

foreach ($orders as $order) {
    $order_riak = $ordersBucket->newObject(strval($order->orderId), $order);
    $order_riak->store();
}
unset($order);

$order_summary_riak = $orderSummariesBucket->newObject(strval($orderSummary->customerId), $orderSummary);
$order_summary_riak->store();


// Fetching related data by shared key

$fetched_customer = $customersBucket->get('1')->data;
$fetched_customer['orderSummary'] = $orderSummariesBucket->get('1')->data;
print("Customer with OrderSummary data: \n");
print_r($fetched_customer);


// Adding Index Data
$keys = array(1,2,3);
foreach($keys as $key) {
    $order = $ordersBucket->get(strval($key));
    $salespersonId = $order->data['salespersonId'];
    $orderDate = $order->data['orderDate'];
    $order->addIndex('SalespersonId','int',$salespersonId);
    $order->addIndex('OrderDate','bin',$orderDate);
    $order->store();
}
unset($key);


// Index Queries

// Query for orders where the SalespersonId int index is set to 9000
$janes_orders = $ordersBucket->indexSearch('SalespersonId', 'int', 9000);
print("\n\nJane's Orders: \n");
print_r($janes_orders);

// Query for orders where the OrderDate bin index is between 2013-10-01 and 2013-10-31
$october_orders = $ordersBucket->indexSearch('OrderDate', 'bin', '2013-10-01', '2013-10-31');
print("\n\nOctober's Orders: \n");
print_r($october_orders);

?>
