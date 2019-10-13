<?php
namespace console\controllers;

use common\models\generated\EpicGamesApplication;
use common\models\generated\Game;
use common\models\generated\GamesPrice;
use common\models\generated\GamesScore;
use common\models\generated\GamesSpecial;
use common\models\generated\GogApplication;
use common\models\generated\SteamApplication;
use common\models\helpers\ParseHelper;
use common\models\helpers\ViberHelper;
use common\models\User;
use yii\base\Exception;
use yii\console\Controller;
use Yii;
use console\models\generated\ParsedEvents;
use yii\db\Transaction;

class ParseController extends Controller{

    private function sendRequest($url, $params, $post=false){
        $ch = curl_init();

        if($post){
            curl_setopt($ch, CURLOPT_POST, 1);
            curl_setopt($ch, CURLOPT_POSTFIELDS, $params);
        }else{
            $url = $url . http_build_query($params);
        }

        curl_setopt( $ch, CURLOPT_AUTOREFERER, TRUE );
        curl_setopt( $ch, CURLOPT_HEADER, 0 );
        curl_setopt( $ch, CURLOPT_RETURNTRANSFER, 1 );
        curl_setopt( $ch, CURLOPT_URL, $url );
        curl_setopt( $ch, CURLOPT_FOLLOWLOCATION, TRUE );
        curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, false);
        curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);

        $response = json_decode(curl_exec( $ch ));
        $error = curl_error($ch);
        $info = curl_getinfo($ch);
        curl_close( $ch );


        return $response;
    }

    public function actionSteamParse(){
        //php yii parse/steam-parse
        ParseHelper::writeln("Parse app list");
        $this->getSteamAppList();
        ParseHelper::writeln("Parse app info");
        $this->getSteamAppInfo();
        ParseHelper::writeln("Parse news for specials");
        $this->getSteamAppNewsForSpecials();
    }

    private function saveSpecial($gameId, $sourceId, $type){
        if($type === ParseHelper::GAME_SPECIAL_TYPES['free weekend']){
            $endDate = strtotime('monday');
        }elseif($type === ParseHelper::GAME_SPECIAL_TYPES['free'] && $sourceId === ParseHelper::GAME_SOURCE['epic']){
            $endDate = strtotime('+6 day');
        }else{
            $endDate = strtotime('+1 day');
        }
        $existingSpecial = GamesSpecial::find()
            ->where(['game_id' => (integer)$gameId])
            ->andWhere(['source_id' => $sourceId])
            ->andWhere(['type_id' => $type])
            ->andWhere(['>=', 'ends_date', time()])
            ->one();

        if(!$existingSpecial){
            $special = new GamesSpecial();
            $special->game_id = (integer)$gameId;
            $special->source_id = $sourceId;
            $special->type_id = $type;
            $special->ends_date = $endDate;
            $special->created = time();
            $special->announced =ParseHelper::NOT_ANNOUNCED_SPECIAL;
            $special->save();
        }
    }

    private function getSteamAppNewsForSpecials(){
        $offset = ParseHelper::BASE_OFFSET;
        $source_id = ParseHelper::GAME_SOURCE['steam'];
        $count = 0;

        do{
            $gameToParse = SteamApplication::find()
                ->select([SteamApplication::tableName().'.app_id', Game::tableName().'.id as game_id', Game::tableName().'.is_free'])
                ->joinWith('game')
                ->where([SteamApplication::tableName().'.ignore' => ParseHelper::NOT_IGNORE_GAME])
                ->andWhere([SteamApplication::tableName().'.type' => ParseHelper::TYPE_GAME])
                ->limit(ParseHelper::BASE_STEP)
                ->offset($offset)->asArray()->all();

            $freeOnWeekend = [];

            foreach ($gameToParse as $app){
                $count++;
                $appId = $app['app_id'];

                $gameNews = $this->parseSteamAppNews($appId);

                if(isset($gameNews->appnews->newsitems))
                foreach ($gameNews->appnews->newsitems as $newsitem){
                    if(ParseHelper::hasFreeWeekendInfo($newsitem->contents) || ParseHelper::hasFreeWeekendInfo($newsitem->title)){
                        $isFreeTitle = isset($freeOnWeekend[$appId]['title']) && $freeOnWeekend[$appId]['title'] === true ? $freeOnWeekend[$appId]['title'] : ParseHelper::isFreeOnWeekend($newsitem->title);
                        $isFreeContent = isset($freeOnWeekend[$appId]['content']) && $freeOnWeekend[$appId]['content'] === true ? $freeOnWeekend[$appId]['content'] : ParseHelper::isFreeOnWeekend($newsitem->contents);
                        $freeOnWeekend[$appId] = ['title' => $isFreeTitle, 'content' => $isFreeContent];

                        if($isFreeTitle || $isFreeContent){
                            $this->saveSpecial($app['game_id'], $source_id, (int)$app['is_free'] === ParseHelper::FREE_GAME ? ParseHelper::GAME_SPECIAL_TYPES['free'] : ParseHelper::GAME_SPECIAL_TYPES['free weekend']);
                        }
                    }
                }

                if($count % 1000 === 0){
                    ParseHelper::writeln($count." parsed!");
                }elseif($count % 250 === 0){
                    ParseHelper::writeln($count." parsed!");
                }
            }

            $offset+= ParseHelper::BASE_STEP;
        }while(($gameToParse !== []));
    }

    private function saveSteamAppReviews($data, $gameId){
        if($data->success && $data->query_summary !== []){
            $this->saveGameScore($data, ParseHelper::GAME_SOURCE['steam'], $gameId);
        }
    }

    private function saveGameScore($data, $sourceId, $gameId){
        $existedScore = GamesScore::findOne(['game_id' => $gameId, 'score_source_id' => $sourceId]);
        if($existedScore){
            $existedScore->loadBySource($data, $sourceId, $gameId);
            $existedScore->save();
        }else{
            $appScore = new GamesScore();
            $appScore->loadBySource($data, $sourceId, $gameId);
            $appScore->save();
        }
    }

    private function getSteamAppReviews($appId){
        $params = [
            'json' => 1,
            'language' => 'all',
        ];

        return $this->sendRequest(ParseHelper::STEAM_GAME_REVIEWS_URL.$appId.'?', $params);
    }

    private function getSteamAppInfo(){
        $offset = ParseHelper::BASE_OFFSET;
        $source_id = ParseHelper::GAME_SOURCE['steam'];
        $count = 0;

        do{
            $appToParse = SteamApplication::find()
                ->joinWith('game')
                ->andWhere(['or',
                [SteamApplication::tableName().'.type'=>ParseHelper::TYPE_GAME],
                [SteamApplication::tableName().'.type'=>null]
                ])
                ->andWhere(['or',
                    [SteamApplication::tableName().'.ignore'=>ParseHelper::NOT_IGNORE_GAME],
                    [SteamApplication::tableName().'.ignore'=>null]
                ])
                ->andWhere(['or',
                    ['<>',Game::tableName().'.ignore', ParseHelper::IGNORE_GAME],
                    [Game::tableName().'.ignore' => null],
                    [Game::tableName().'.id' => null],
                ])
                ->limit(ParseHelper::BASE_STEP)->offset($offset)->asArray()->all();

            foreach ($appToParse as $app){
                $count++;
                $appId = $app['app_id'];

                $sourceGame = $this->parseSteamAppInfo($appId);

                if((int)$sourceGame->{$appId}->data->steam_appid === (int)$appId && ParseHelper::isGameNameValid($sourceGame->{$appId}->data->name)){
                    $existedApp = SteamApplication::findOne(['app_id' => $appId]);
                    if(isset($existedApp) && $existedApp->type === null){
                        $existedApp->type = $sourceGame->{$appId}->data->type;
                        $existedApp->save();
                    }
                    if($sourceGame->{$appId}->success){
                        $game = self::findGameByName($existedApp->name);
                        if($game){
                            $game->loadFromSource($sourceGame->{$appId}->data,$source_id, $app['id']);
                            $game->save();
                        }else{
                            $game = $this->saveGame($sourceGame->{$appId}->data, $source_id, $app['id']);
                        }

                        if(isset($sourceGame->{$appId}->data->price_overview) || $sourceGame->{$appId}->data->is_free === true){
                            $this->saveSteamGamePrice($sourceGame,$appId, $source_id, $game);
                        }
                    }
                }else{
                    $existedApp = SteamApplication::findOne(['app_id' => $appId]);
                    $existedApp->ignore = ParseHelper::IGNORE_GAME;
                    $existedApp->save(false);
                }

                if($count % 1000 === 0){
                    ParseHelper::writeln($count." parsed!");
                }elseif($count % 100 === 0){
                    ParseHelper::writeln($count." parsed!");
                }
            }
            $offset += ParseHelper::BASE_STEP;
        }while(($appToParse !== []));
    }

    private function saveSteamGamePrice($sourceGame, $appId,$source_id,$game){
        $transaction = Yii::$app->db->beginTransaction();

        GamesPrice::updateAll([GamesPrice::tableName().'.is_actual' => ParseHelper::NOT_ACTUAL_PRICE],[GamesPrice::tableName().'.game_id' => $game->id,GamesPrice::tableName().'.source_id' => $source_id]);

        $gamePrice = GamesPrice::findOne(['source_id' => $source_id, 'game_id' => $game->id]);
        if(!$gamePrice){
            $gamePrice = new GamesPrice();
        }

        $priceData = isset($sourceGame->{$appId}->data->price_overview) ? $sourceGame->{$appId}->data->price_overview : null;
        $isFree = isset($sourceGame->{$appId}->data->is_free) ? $sourceGame->{$appId}->data->is_free : false;

        $gamePrice->loadFromSource($priceData, $source_id, $isFree);
        $gamePrice->game_url = ParseHelper::STEAM_GAME_URL.$appId;
        $gamePrice->game_id = $game->id;
        $gamePrice->save();

        $transaction->commit();
        if($gamePrice->base_price < ParseHelper::GAME_LOWEST_PRICE_IN_RUB && $gamePrice->base_price !== null && $gamePrice->base_price !== 0){
            $game->ignore = ParseHelper::IGNORE_GAME;
            $game->save();
        }else{
            $game->ignore = ParseHelper::NOT_IGNORE_GAME;
            $game->save();
        }
    }

    private function parseSteamAppNews($appId){
        $params = [
            'appid' => $appId,
            'count' => ParseHelper::STEAM_GAME_NEWS_BASE_COUNT,
        ];

        return $this->sendRequest(ParseHelper::STEAM_GAME_NEWS_URL, $params);
    }

    private function parseSteamAppInfo($appId){
        $params = [
            'appids' => $appId,
            'cc' => ParseHelper::BASE_CC,
        ];

        return $this->sendRequest(ParseHelper::STEAM_GAME_INFO_URL, $params);
    }

    private function getSteamAppList(){
        $games = $this->sendRequest(ParseHelper::STEAM_GAME_LIST_URL, []);
        $count = 0;

        try{
            $transaction = null;
            $applicationsArr = array_chunk($games->applist->apps, 1000);
            foreach ($applicationsArr as $applications){
                $transaction = Yii::$app->db->beginTransaction();
                foreach ($applications as $application){
                    $count++;
                    $existedApp = SteamApplication::findOne(['app_id' => $application->appid]);
                    if(!$existedApp){
                        $newApp = new SteamApplication();
                        $newApp->name = ParseHelper::removeEmoji($application->name);
                        $newApp->app_id = $application->appid;
                        $newApp->save();
                    }
                }
                ParseHelper::writeln($count." parsed!");
                $transaction->commit();
            }
        }catch (\Exception $e){
            ParseHelper::writeln($e->getMessage());
            if($transaction !== null)
                $transaction->rollBack();
        }


    }

    private function getSteamScore(){
        $offset = ParseHelper::BASE_OFFSET;
        $source_id = ParseHelper::GAME_SCORE_SOURCE['steam'];
        $count = $offset;

        do{
            $appToParse = SteamApplication::find()
                ->joinWith('game')
                ->andWhere(['or',
                    [SteamApplication::tableName().'.type'=>ParseHelper::TYPE_GAME],
                    [SteamApplication::tableName().'.type'=>null]
                ])
                ->andWhere(['or',
                    ['<>',Game::tableName().'.ignore', ParseHelper::IGNORE_GAME],
                    [Game::tableName().'.ignore' => null],
                    [Game::tableName().'.id' => null],
                ])
                ->limit(ParseHelper::BASE_STEP)->offset($offset)->asArray()->all();

            foreach ($appToParse as $app){
                $count++;
                $appId = $app['app_id'];

                $game = Game::findOne(['game_source_id' => $source_id, 'game_id' => $app['id']]);
                $this->saveSteamAppReviews($this->getSteamAppReviews($appId),$game->id);

                if($count % 1000 === 0){
                    ParseHelper::writeln($count." parsed!");
                }elseif($count % 100 === 0){
                    ParseHelper::writeln($count." parsed!");
                }
            }

            $offset += ParseHelper::BASE_STEP;
        }while(($appToParse !== []));
    }

	private function findGameByName($name){
		return Game::find()->where(['=', 'name', $name])->one();
	}
	
    private function saveGame($gameData, $sourceId, $appId){
        $game = new Game();
        $game->loadFromSource($gameData,$sourceId, $appId);
        $game->save();

        return $game;
    }
}