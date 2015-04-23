<?php

include_once 'vendor/autoload.php';

use Basho\Riak;
use Basho\Riak\Location;
use Basho\Riak\Node;
use Basho\Riak\Command;

$node = (new Node\Builder)
    ->atHost('127.0.0.1')
    ->onPort(8098)
    ->build();

$riak = new Riak([$node]);

// Class definitions for our models

class Customer
{
    var $customerId;
    var $name;
    var $address;
    var $city;
    var $state;
    var $zip;
    var $phone;
    var $createdDate;
}

class Order
{
    public function __construct()
    {
        $this->items = array();
    }
    var $orderId;
    var $customerId;
    var $salespersonId;
    var $items;
    var $total;
    var $orderDate;
}

class Item
{
    public function __construct($itemId, $title, $price)
    {
        $this->itemId = $itemId;
        $this->title = $title;
        $this->price = $price;
    }
    var $itemId;
    var $title;
    var $price;
}

class OrderSummary
{
    public function __construct() 
    {
        $this->summaries = array();
    }
    var $customerId;
    var $summaries;
}

class OrderSummaryItem
{
    public function __construct(Order $order) 
    {
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


$orders = [];

$order1 = new Order();
$order1->orderId = 1;
$order1->customerId = 1;
$order1->salespersonId = 9000;
$order1->items = [
    new Item(
        'TCV37GIT4NJ',
        'USB 3.0 Coffee Warmer',
        15.99
    ),
    new Item(
        'PEG10BBF2PP', 
        'eTablet Pro; 24GB; Grey', 
        399.99
    )
];
$order1->total = 415.98;
$order1->orderDate = '2013-10-01 14:42:26';
$orders[] = $order1;

$order2 = new Order();
$order2->orderId = 2;
$order2->customerId = 1;
$order2->salespersonId = 9001;
$order2->items = [
    new Item(
        'OAX19XWN0QP',
        'GoSlo Digital Camera',
        359.99
    )
];
$order2->total = 359.99;
$order2->orderDate = '2013-10-15 16:43:16';
$orders[] = $order2;

$order3 = new Order();
$order3->orderId = 3;
$order3->customerId = 1;
$order3->salespersonId = 9000;
$order3->items = [
    new Item(
        'WYK12EPU5EZ',
        'Call of Battle = Goats - Gamesphere 4',
        69.99
    ),
    new Item(
        'TJB84HAA8OA',
        'Bricko Building Blocks',
        4.99
    )
];
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
$node = (new Node\Builder)
    ->atHost('127.0.0.1')
    ->onPort(8098)
    ->build();

$riak = new Riak([$node]);

// Creating Buckets
$customersBucket = new Riak\Bucket('Customers');
$ordersBucket = new Riak\Bucket('Orders');
$orderSummariesBucket = new Riak\Bucket('OrderSummaries');

// Storing Data
$storeCustomer = (new Command\Builder\StoreObject($riak))
    ->buildJsonObject($customer)
    ->atLocation(new Location($customer->customerId, $customersBucket))
    ->build();
$storeCustomer->execute();

foreach ($orders as $order) {
    $storeOrder = (new Command\Builder\StoreObject($riak))
        ->buildJsonObject($order)
        ->atLocation(new Location($order->orderId, $ordersBucket))
        ->build();
    $storeOrder->execute();
}
unset($order);

$storeSummary = (new Command\Builder\StoreObject($riak))
    ->buildJsonObject($orderSummary)
    ->atLocation(new Location($orderSummary->customerId, $orderSummariesBucket))
    ->build();
$storeSummary->execute();


// Fetching related data by shared key
$fetched_customer = (new Command\Builder\FetchObject($riak))
                    ->atLocation(new Location('1', $customersBucket))
                    ->build()->execute()->getObject()->getData();

$fetched_customer->orderSummary =
    (new Command\Builder\FetchObject($riak))
    ->atLocation(new Location('1', $orderSummariesBucket))
    ->build()->execute()->getObject()->getData();

print("Customer with OrderSummary data: \n");
print_r($fetched_customer);


// Adding Index Data
$keys = array(1,2,3);
foreach ($keys as $key) {
    $orderLocation = new Location($key, $ordersBucket);
    $orderObject = (new Command\Builder\FetchObject($riak))
                    ->atLocation($orderLocation)
                    ->build()->execute()->getObject();

    $order = $orderObject->getData();

    $orderObject->addValueToIndex('SalespersonId_int', $order->salespersonId);
    $orderObject->addValueToIndex('OrderDate_bin', $order->orderDate);

    $storeOrder = (new Command\Builder\StoreObject($riak))
                    ->withObject($orderObject)
                    ->atLocation($orderLocation)
                    ->build();
    $storeOrder->execute();
}
unset($key);


// Index Queries

// Query for orders where the SalespersonId int index is set to 9000
$fetchIndex = (new Command\Builder\QueryIndex($riak))
                ->inBucket($ordersBucket)
                ->withIndexName('SalespersonId_int')
                ->withScalarValue(9000)->build();
$janes_orders = $fetchIndex->execute()->getResults();

print("\n\nJane's Orders: \n");
print_r($janes_orders);

// Query for orders where the OrderDate bin index is 
// between 2013-10-01 and 2013-10-31
$fetchOctoberOrders = (new Command\Builder\QueryIndex($riak))
                        ->inBucket($ordersBucket)
                        ->withIndexName('OrderDate_bin')
                        ->withRangeValue('2013-10-01','2013-10-31')
                        ->withReturnTerms(true)
                        ->build();

$octobers_orders = $fetchOctoberOrders->execute()->getResults();

print("\n\nOctober's Orders: \n");
print_r($octobers_orders);


// Cleanup all our data
(new Command\Builder\DeleteObject($riak))
    ->atLocation(new Location($customer->customerId, $customersBucket))
    ->build()
    ->execute();
    
foreach ($orders as $order) {
    (new Command\Builder\DeleteObject($riak))
        ->atLocation(new Location($order->orderId, $ordersBucket))
        ->build()
        ->execute();
}
unset($order);

(new Command\Builder\DeleteObject($riak))
    ->atLocation(new Location($orderSummary->customerId, $orderSummariesBucket))
    ->build()
    ->execute();
?>
