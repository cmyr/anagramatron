
import sys
from datetime import datetime
import multiprocessing

from . import twitterhandler, streamhandler, anagramfinder, hit_server

def run(server_only=False):
    try:
        import setproctitle
        setproctitle.setproctitle('anagramatron')
    except ImportError:
        print("missing module: setproctitle")
        pass

    if server_only:
        hit_server.start_hit_server()
    else:
        hitserver = multiprocessing.Process(target=hit_server.start_hit_server)
        hitserver.daemon = True
        hitserver.start()

        hit_manager = hitmanager.HitDBManager('hitdata2en.db')
        
        def handle_hit(p1, p2):
            hit_manager.new_hit(p1, p2)

        anagram_finder = anagramfinder.AnagramFinder(hit_callback=handle_hit)
        
        while 1:
            try:
                print('starting stream handler')
                stream_handler = stream.StreamHandler()
                stream_handler.start()
                for processed_tweet in stream_handler:
                    anagram_finder.handle_input(processed_tweet)
                    stats.update_console()

            except anagramfinder.NeedsMaintenance:
                logging.debug('caught NeedsMaintenance exception')
                print('performing maintenance')
                stream_handler.close()
                anagram_finder.perform_maintenance()

            except KeyboardInterrupt:
                stream_handler.close()
                anagram_finder.close()
                return 0

            except Exception as err:
                stream_handler.close()
                anagram_finder.close()
                twitterhandler.TwitterHandler().send_message(str(err) +
                                              "\n" +
                                              datetime.today().isoformat())
                raise


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-s', '--server-only',
        help="run in server mode only",
        action="store_true")
    args = parser.parse_args()

    return run(args.server_only)


if __name__ == "__main__":
    main()
