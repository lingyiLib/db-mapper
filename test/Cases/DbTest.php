<?php


namespace lingyiLib\DbMapper\Cases;

use App\Entity\Post;
use DateTime;
use HyperfTest\HttpTestCase;
use lingyiLib\DbMapper\DB;

/**
 * @internal
 */
class DbTest extends HttpTestCase
{
//    public function testIndex(){
//        $ret = \Hyperf\DbConnection\Db::table('posts')->where(['title'=>'test'])->first();
//        $this->assertEquals(82,$ret->id);
//    }
    public function testInsert()
    {
        $mapper = DB::mapper(Post::class);

        $result = $mapper->create([
            'id'=>1,
            'titleTest'=>'create测试',
            'body'=>'create内容',
            'status'=>1,
            'author_id'=>2,
            'created_at'=>new DateTime('2022-05-22 06:12:42')
        ]);

        $post = $result->dataUnmodified();
        $this->assertEquals([
            'titleTest'=>'create测试',
            'created_at'=>new Datetime('2022-05-22 06:12:42'),
            'status'=>1,
            'author_id'=>2,
            'body'=>'create内容',
            'id'=>1
        ],$post);

        $result = $mapper->insert([
            'id'=>2,
            'titleTest'=>'insert测试',
            'body'=>'insert内容',
            'status'=>0,
            'author_id'=>2,
            'created_at'=>new Datetime('2022-05-22 06:22:35')
        ]);
        $this->assertEquals(2,$result);

        // Insert and return record primary key, or boolean false
        $entity = $mapper->build([
            'id'=>3,
            'titleTest'=>'insert测试2',
            'body'=>'insert内容2',
            'status'=>0,
            'author_id'=>2,
            'created_at'=>new DateTime('2022-05-26 01:14:14')
        ]);
        $result = $mapper->insert($entity);
        $this->assertEquals(3,$result);

        $entity = $mapper->build([
            'id'=>4,
            'titleTest'=>'save测试',
            'body'=>'save内容',
            'status'=>0,
            'author_id'=>2,
            'created_at'=>new DateTime('2022-05-26 01:15:58')
        ]);
        $result = $mapper->save($entity);
        $this->assertEquals(4,$result);
        if ($result) {
            // Update with another call to save
            $entity->body = 'Lester Tester';
            $mapper->save($entity);
        }
    }

    public function testUpdate()
    {
        $mapper = DB::mapper(Post::class);
        // Find and update an entity
        $entity = $mapper->first(['titleTest' => 'save测试']);
        if ($entity) {
            $entity->titleTest = 'Lester Tester';
            $mapper->update($entity);
        }
        $post = $mapper->first(['titleTest' => "Lester Tester"]);
        $this->assertEquals("Lester Tester", $post->titleTest);
        $entity->titleTest = 'insert测试';
        $mapper->update($entity);
    }

    public function testRead()
    {
        $mapper = DB::mapper(Post::class);

        // Get Post with 'id' = 58
        $post = $mapper->get(1);
        $this->assertIsObject($post);
        $this->assertEquals(1, $post->id);

        // Get ALL posts
        $posts = $mapper->all();
        foreach($posts as $post){
            $this->assertIsObject($post);
            $item = $post->dataUnmodified();
            $this->assertArrayHasKey("id", $item);
            $this->assertArrayHasKey("titleTest", $item);
            $this->assertArrayHasKey("body", $item);
            $this->assertArrayHasKey("status", $item);
            $this->assertArrayHasKey("author_id", $item);
            $this->assertArrayHasKey("created_at", $item);
        }

        $count = $mapper->all()->count();
        $this->assertEquals(4, $count);

        $post = $mapper->where(['status' => 1])->first();
        $this->assertIsObject($post);
        $this->assertEquals(1, $post->id);
        $post = $post->dataUnmodified();
        $this->assertArrayHasKey("id", $post);
        $this->assertArrayHasKey("titleTest", $post);
        $this->assertArrayHasKey("body", $post);
        $this->assertArrayHasKey("status", $post);
        $this->assertArrayHasKey("author_id", $post);
        $this->assertArrayHasKey("created_at", $post);

        $posts = $mapper->all()->where(['status' => 1]);
        foreach($posts as $post){
            $this->assertIsObject($post);
            $this->assertEquals(1, $post->id);
            $post = $post->dataUnmodified();
            $this->assertArrayHasKey("id", $post);
            $this->assertArrayHasKey("titleTest", $post);
            $this->assertArrayHasKey("body", $post);
            $this->assertArrayHasKey("status", $post);
            $this->assertArrayHasKey("author_id", $post);
            $this->assertArrayHasKey("created_at", $post);
        }

        $post = $mapper->select(['title','status','created_at'])->where(['status' => 1])->first();
        $post = $post->dataUnmodified();
        $this->assertEquals([
            'titleTest'=>"create测试",
            'created_at'=>new Datetime('2022-05-22 06:12:42'),
            'status'=>1,
            'author_id'=>null,
            'body'=>null,
            'id'=>null
        ],$post);

        $count = $mapper->all()
            ->where(['status' => 0])
            ->order(['id' => 'DESC'])->count();

        $this->assertEquals(3, $count);

        $count = $mapper->all()
            ->where(['created_at <' => new \DateTime('-3 days')])->count();

        $this->assertEquals(4, $count);

        $count = $mapper->all()
            ->where(['id' => [1,5]])->count();
        $this->assertEquals(1, $count);

        $post = $mapper->first(['body' => "Lester Tester"]);
        $this->assertEquals("Lester Tester", $post->body);

        $post = $mapper->where(['titleTest' => "insert测试"])->first();
        $this->assertEquals("insert测试", $post->titleTest);
    }

    public function testTransaction()
    {
        //测试rollback
        $mapper = DB::mapper(Post::class);
        DB::beginTransaction();
        $result = $mapper->insert([
            'id'=>5,
            'titleTest'=>'insert测试',
            'body'=>'insert内容',
            'status'=>0,
            'author_id'=>2,
            'created_at'=>new Datetime('2022-05-22 06:22:35')
        ]);
        $this->assertEquals(5, $result);
        DB::rollback();

        //测试commit
        $count = $mapper->all()->count();
        $this->assertEquals(4, $count);
        DB::beginTransaction();
        $result = $mapper->insert([
            'id'=>5,
            'titleTest'=>'insert测试',
            'body'=>'insert内容',
            'status'=>0,
            'author_id'=>2,
            'created_at'=>new Datetime('2022-05-22 06:22:35')
        ]);
        $this->assertEquals(5, $result);
        DB::commit();
        $mapper = DB::mapper(Post::class);
        $mapper->transaction(function($mapper){
                return $mapper->insert([
                    'id'=>6,
                    'titleTest'=>'insert测试',
                    'body'=>'insert内容',
                    'status'=>0,
                    'author_id'=>2,
                    'created_at'=>new Datetime('2022-05-22 06:22:35')
                ]);
        });

        $count = $mapper->all()->count();
        $this->assertEquals(6, $count);
    }

    public function testDelete()
    {
        $result = DB::mapper(Post::class)->delete();
        $this->assertEquals(6, $result);
    }

}