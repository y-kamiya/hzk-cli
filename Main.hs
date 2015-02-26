{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE QuasiQuotes #-}
{-# OPTIONS_GHC -fno-cse #-}

import qualified Database.Zookeeper as Z
import System.Console.CmdArgs
import System.Environment (getArgs, lookupEnv)
import Text.Heredoc (str)
import Data.Maybe (fromMaybe)
import qualified Data.ByteString.Char8 as C

data Command = 
   Create {
     endpoint :: String
   , path :: String
   , content :: String
   }
 | Get {
     endpoint :: String
   , path :: String
   }
 | Delete {
     endpoint :: String
   , path :: String
   }
 | List {
     endpoint :: String
   , path :: String
   }
   deriving (Eq, Show, Data, Typeable)

detail = details . lines

createO = Create {
     endpoint = def
   , path = def &= argPos 1
   , content = def &= argPos 2
   } &= name "create"                                              
     &= detail [str|create znode
                   |                                               
                   |Examples:                                      
                   |    % export ZK_ENDPOINT=127.0.0.1:2181
                   |    % zk-cli create /test "contents"
                   |]                                              

getO = Get {
     endpoint = def
   , path = def &= argPos 1
   } &= name "get"                                              
     &= detail [str|get znode
                   |                                               
                   |Examples:                                      
                   |    % export ZK_ENDPOINT=127.0.0.1:2181
                   |    % zk-cli get /test 
                   |]                                              

deleteO = Delete {
     endpoint = def
   , path = def &= argPos 1
   } &= name "delete"                                              
     &= detail [str|delete znode
                   |                                               
                   |Examples:                                      
                   |    % export ZK_ENDPOINT=127.0.0.1:2181
                   |    % zk-cli delete /test 
                   |]                                              

listO = List {
     endpoint = def
   , path = def &= argPos 1
   } &= name "list"                                              
     &= detail [str|get znode
                   |                                               
                   |Examples:                                      
                   |    % export ZK_ENDPOINT=127.0.0.1:2181
                   |    % zk-cli list /test 
                   |]                                              

commands = [ createO
           , getO
           , deleteO
           , listO
           ]

main :: IO ()
main = do
  cmd <- cmdArgs $ modes commands
  mep <- lookupEnv "ZK_ENDPOINT"
  let ep = fromMaybe "" mep
  execute $ cmd { endpoint = ep }

execute :: Command -> IO ()
execute (Create endpoint path content) = 
  Z.withZookeeper endpoint 1000 Nothing Nothing $ \zh -> do
    e <- Z.create zh path (Just $ C.pack content) Z.OpenAclUnsafe []
    case e of
      Left err -> print err
      Right s -> print s
   
execute (Get endpoint path) = 
  Z.withZookeeper endpoint 1000 Nothing Nothing $ \zh -> do
    e <- Z.get zh path Nothing  
    case e of
      Left err -> print err
      Right (content, _) -> print content

execute (Delete endpoint path) = 
  Z.withZookeeper endpoint 1000 Nothing Nothing $ \zh -> do
    e <- Z.delete zh path Nothing  
    case e of
      Left err -> print err
      Right _ -> print "deleted"
   
execute (List endpoint path) = 
  Z.withZookeeper endpoint 1000 Nothing Nothing $ \zh -> do
    e <- Z.getChildren zh path Nothing  
    case e of
      Left err -> print err
      Right ss -> print ss
   
   
