<?php 
require_once 'riak-php-client/src/Basho/Riak/Riak.php';
require_once 'riak-php-client/src/Basho/Riak/Bucket.php';
require_once 'riak-php-client/src/Basho/Riak/Exception.php';
require_once 'riak-php-client/src/Basho/Riak/Link.php';
require_once 'riak-php-client/src/Basho/Riak/MapReduce.php';
require_once 'riak-php-client/src/Basho/Riak/Object.php';
require_once 'riak-php-client/src/Basho/Riak/StringIO.php';
require_once 'riak-php-client/src/Basho/Riak/Utils.php';
require_once 'riak-php-client/src/Basho/Riak/Link/Phase.php';
require_once 'riak-php-client/src/Basho/Riak/MapReduce/Phase.php';

$client = new Basho\Riak\Riak('127.0.0.1', 8098);

// Note: Use this line instead of the former if using a local devrel cluster
//$client = new Basho\Riak\Riak('127.0.0.1', 10018);

// Creating Objects In Riak
print('Creating Objects In Riak...\n');

$myBucket = $client->bucket('test');

$val1 = 1;
$obj1 = $myBucket->newObject('one', $val1);
$obj1->store();

$val2 = 'two';
$obj2 = $myBucket->newObject('two', $val2);
$obj2->store();

$val3 = array('myValue' => 3);
$obj3 = $myBucket->newObject('three', $val3);
$obj3->store();

// Reading Objects From Riak
print('Reading Objects From Riak...\n');

$fetched1 = $myBucket->get('one');
$fetched2 = $myBucket->get('two');
$fetched3 = $myBucket->get('three');

assert($val1 == $fetched1->getData());
assert($val2 == $fetched2->getData());
assert($val3 == $fetched3->getData());

// Updating Objects In Riak
print('Updating Objects In Riak...\n');

$fetched3->data['myValue'] = 42;
$fetched3->store();

// Deleting Objects From Riak
print('Deleting Objects From Riak...\n');

$fetched1->delete();
$fetched2->delete();
$fetched3->delete();

// Working With Complex Objects
print('Working With Complex Objects...\n');

class Book
{
    var $title;
    var $author;
    var $body;
    var $isbn;
    var $copiesOwned;
}

$book = new Book();
$book->isbn = '1111979723';
$book->title = 'Moby Dick';
$book->author = 'Herman Melville';
$book->body = 'Call me Ishmael. Some years ago...';
$book->copiesOwned = 3;

$booksBucket = $client->bucket('books');
$newBook = $booksBucket->newObject($book->isbn, $book);
$newBook->store();

$riakObject = $booksBucket->getBinary($book->isbn);
print('Serialized Object:');
print($riakObject->data);

$newBook->delete();

?>
