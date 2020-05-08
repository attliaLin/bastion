use bastion::prelude::*;
use rayon::prelude::*;
use std::time::Duration;

#[macro_use]
extern crate log;

mod prime_number_impl {
    use super::PrimeResponse;
    use std::iter;
    use std::time::Instant;

    // in order to determine if n is prime
    // we will use a primality test.
    // https://en.wikipedia.org/wiki/Primality_test#Pseudocode
    fn is_prime(n: u128) -> bool {
        if n <= 3 {
            n > 1
        } else if n % 2 == 0 || n % 3 == 0 {
            false
        } else {
            let approximated_square_root = (((n >> 16) as f64).sqrt() as u128) << 16;
            for i in (5..=approximated_square_root).step_by(6) {
                if n % i == 0 || n % (i + 2) == 0 {
                    return false;
                }
            }
            true
        }
    }

    // given a sequence of digits, return the corresponding number
    // eg: assert_eq!(1234, digits_to_number(vec![1,2,3,4]))
    fn digits_to_number(iter: impl Iterator<Item = usize>) -> u128 {
        iter.fold(0, |acc, b| acc * 10 + b as u128)
    }

    fn get_min_bound(num_digits: usize) -> u128 {
        let lower_bound_iter =
            iter::once(1usize).chain(iter::repeat(0usize).take(num_digits - 1 as usize));
        digits_to_number(lower_bound_iter)
    }

    fn get_max_bound(num_digits: usize) -> u128 {
        let lower_bound_iter = iter::once(1usize).chain(iter::repeat(0usize).take(num_digits));
        digits_to_number(lower_bound_iter)
    }

    fn number_or_panic(number_to_return: u128) -> u128 {
        // Let's roll a dice
        if false && rand::random::<u8>() % 6 == 0 {
            panic!(format!(
                "I was about to return {} but I chose to panic instead!",
                number_to_return
            ))
        }
        number_to_return
    }

    fn get_prime(num_digits: usize) -> u128 {
        let min_bound = get_min_bound(num_digits);
        // with num_digits = 4, max_bound == 10000
        let max_bound = get_max_bound(num_digits);
        // maybe_prime is a number in range [1000, 10000)
        // the closing parenthesiss means it won't reach the number.
        // the maximum allowed value for maybe_prime is 9999.
        use rand::Rng;
        let mut maybe_prime = rand::thread_rng().gen_range(min_bound, max_bound);
        loop {
            if is_prime(maybe_prime.into()) {
                return number_or_panic(maybe_prime);
            }
            // for any integer n > 3,
            // there always exists at least one prime number p
            // with n < p < 2n - 2
            maybe_prime += 1;
            // We don't want to return a number
            // that doesn't have the right number of digits
            if maybe_prime == max_bound {
                maybe_prime = min_bound;
            }
        }
    }

    pub fn prime_number(num_digits: usize) -> PrimeResponse {
        // Start a stopwatch
        let start = Instant::now();
        // Get a prime number
        let prime_number = get_prime(num_digits);
        // Stop the stopwatch
        let elapsed = Instant::now().duration_since(start);

        PrimeResponse {
            prime_number,
            num_digits,
            compute_time: elapsed,
        }
    }
}

mod prime_numbers_order {
    use super::PrimeResponse;
    use bastion::prelude::*;

    pub struct Request {
        child_id: usize,
        num_digits: usize,
    }

    pub struct Result {
        pub child_id: BastionId,
        pub response: PrimeResponse,
    }
}

#[derive(Debug)]
pub struct PrimeResponse {
    prime_number: u128,
    num_digits: usize,
    compute_time: Duration,
}

impl std::fmt::Display for PrimeResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} is a prime number that has {} digits. If was found in {}s and {}ms",
            self.prime_number,
            self.num_digits,
            self.compute_time.as_secs(),
            self.compute_time.as_millis() - (self.compute_time.as_secs() as u128 * 1000)
        )
    }
}

async fn serve_prime_numbers(ctx: BastionContext) -> Result<(), ()> {
    let arc_ctx = std::sync::Arc::new(ctx);
    loop {
        msg! { arc_ctx.clone().recv().await?,
            nb_digits: usize =!> {
                let ctx2 = arc_ctx.clone();
                std::thread::spawn(move || answer!(ctx2, prime_number_impl::prime_number(nb_digits)).expect("couldn't reply :("));
            };
            unknown:_ => {
                error!("uh oh, I received a message I didn't understand\n {:?}", unknown);
            };
        }
    }
}

async fn request_prime(
    children: std::sync::Arc<ChildrenRef>,
    child_index: usize,
    nb_digits: usize,
) -> std::io::Result<PrimeResponse> {
    let reply = children
        .elems()
        .get(child_index)
        .expect("couldn't find child")
        .ask_anonymously(nb_digits)
        .expect("couldn't perform request");
    msg! { reply.await.expect("couldn't receive reply"),
        prime_response: PrimeResponse => {
            Ok(prime_response)
        };
        unknown:_ => {
            error!("uh oh, I received a message I didn't understand: {:?}", unknown);
            Err(std::io::Error::new(std::io::ErrorKind::Other, "unknown reply"))
        };
    }
}

// serve digits will allow us
// to have enough work for each child
// so no one is bored :D
fn serve_digits(nb_children: usize) -> Vec<usize> {
    let max = 15usize;
    let mut list = std::iter::repeat(1..=max)
        .flatten()
        .take(max * nb_children)
        .collect::<Vec<_>>();
    list.sort();
    list.reverse();
    list
}

// RUST_LOG=info cargo run --example prime_numbers
fn main() {
    env_logger::init();

    Bastion::init();
    Bastion::start();

    // Let's create a supervisor that will watch out for children
    // and dispatch requests
    let supervisor: SupervisorRef = Bastion::supervisor(|sp| {
        sp.with_strategy(SupervisionStrategy::OneForOne)
        // ...and return it.
    })
    .expect("Couldn't create the supervisor.");

    // Let's create children that will serve prime numbers
    let nb_children = num_cpus::get();

    rayon::ThreadPoolBuilder::new()
        .num_threads(nb_children)
        .build_global()
        .unwrap();

    // We're using children_ref here
    // because we want to keep the children adress.
    // It will allow us to send message to specific children.
    let children_ref = std::sync::Arc::new(
        supervisor
            .children(|children| {
                children
                    .with_redundancy(nb_children)
                    .with_exec(serve_prime_numbers)
            })
            .expect("couldn't create children"),
    );

    let total_timer = std::time::Instant::now();

    serve_digits(nb_children)
        .into_par_iter()
        .enumerate()
        .map(|(index, nb_digits)| {
            let child_number = index % children_ref.elems().len();
            let children_ref = children_ref.clone();
            let task = spawn!(request_prime(children_ref, child_number, nb_digits));
            (child_number, task)
        })
        // we are collecting here
        // to make sure all tasks have been spawned
        // before we run! them
        .collect::<Vec<_>>()
        .into_par_iter()
        .for_each(|(child_number, task)| {
            let response = run!(task).expect("task failed").expect("wrong response");
            info!("child #{:02} | {}", child_number, response);
        });

    println!(
        "total duration {}s {}ms",
        total_timer.elapsed().as_secs(),
        total_timer.elapsed().as_millis() - (total_timer.elapsed().as_secs() as u128 * 1000)
    );

    Bastion::stop();
    Bastion::block_until_stopped();
}
